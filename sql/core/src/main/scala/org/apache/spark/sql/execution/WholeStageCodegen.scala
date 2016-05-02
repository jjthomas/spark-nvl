/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util

import edu.mit.nvl.llvm.runtime.LlvmCompiler
import edu.mit.nvl.parser.NvlParser
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, OffHeapColumnVector}
import org.apache.spark.{broadcast, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toCommentSafeString
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, SortMergeJoin}
import org.apache.spark.sql.execution.metric.{LongSQLMetricValue, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * An interface for those physical operators that support codegen.
 */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: TungstenAggregate => "agg"
    case _: BroadcastHashJoin => "bhj"
    case _: SortMergeJoin => "smj"
    case _: PhysicalRDD => "rdd"
    case _: DataSourceScan => "scan"
    case _ => nodeName.toLowerCase
  }

  /**
   * Creates a metric using the specified name.
   *
   * @return name of the variable representing the metric
   */
  def metricTerm(ctx: CodegenContext, name: String): String = {
    val metric = ctx.addReferenceObj(name, longMetric(name))
    val value = ctx.freshName("metricValue")
    val cls = classOf[LongSQLMetricValue].getName
    ctx.addMutableState(cls, value, s"$value = ($cls) $metric.localValue();")
    value
  }

  /**
   * Whether this SparkPlan support whole stage codegen or not.
   */
  def supportCodegen: Boolean = true

  /**
   * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
   */
  protected var parent: CodegenSupport = null

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * Note: right now we support up to two RDDs.
   */
  def upstreams(): Seq[RDD[InternalRow]]

  /**
   * Returns Java source code to process the rows from upstream.
   */
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    waitForSubqueries()
    s"""
       |/*** PRODUCE: ${toCommentSafeString(this.simpleString)} */
       |${doProduce(ctx)}
     """.stripMargin
  }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   *   if (!initialized) {
   *     # create a hash map, then build the aggregation hash map
   *     # call child.produce()
   *     initialized = true;
   *   }
   *   while (hashmap.hasNext()) {
   *     row = hashmap.next();
   *     # build the aggregation results
   *     # create variables for results
   *     # call consume(), which will call parent.doConsume()
   *      if (shouldStop()) return;
   *   }
   */
  protected def doProduce(ctx: CodegenContext): String

  /**
   * Consume the generated columns or row from current SparkPlan, call it's parent's doConsume().
   */
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    val inputVars =
      if (row != null) {
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).gen(ctx)
        }
      } else {
        assert(outputVars != null)
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeRow, so we should copy them
        outputVars.map(_.copy())
      }
    val rowVar = if (row != null) {
      ExprCode("", "false", row)
    } else {
      if (outputVars.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(outputVars)
        // generate the code to create a UnsafeRow
        ctx.currentVars = outputVars
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        val code = s"""
          |$evaluateInputs
          |${ev.code.trim}
         """.stripMargin.trim
        ExprCode(code, "false", ev.value)
      } else {
        // There is no columns
        ExprCode("", "false", "unsafeRow")
      }
    }

    ctx.freshNamePrefix = parent.variablePrefix
    val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs)
    s"""
       |
       |/*** CONSUME: ${toCommentSafeString(parent.simpleString)} */
       |$evaluated
       |${parent.doConsume(ctx, inputVars, rowVar)}
     """.stripMargin
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent
   * them to be evaluated twice.
   */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code != "").map(_.code.trim).mkString("\n")
    variables.foreach(_.code = "")
    evaluate
  }

  /**
   * Returns source code to evaluate the variables for required attributes, and clear the code
   * of evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code != "" && required.contains(attributes(i))) {
        evaluateVars.append(ev.code.trim + "\n")
        ev.code = ""
      }
    }
    evaluateVars.toString()
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Generate the Java source code to process the rows from child SparkPlan.
   *
   * This should be override by subclass to support codegen.
   *
   * For example, Filter will generate the code like this:
   *
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (isNull1 || !value2) continue;
   *   # call consume(), which will call parent.doConsume()
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }
}


/**
 * InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
 *
 * This is the leaf node of a tree with WholeStageCodegen, is used to generate code that consumes
 * an RDD iterator of InternalRow.
 */
case class InputAdapter(child: SparkPlan) extends UnaryNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // Right now, InputAdapter is only used when there is one upstream.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val row = ctx.freshName("row")
    s"""
       | while ($input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  override def simpleString: String = "INPUT"

  override def treeChildren: Seq[SparkPlan] = Nil
}

object WholeStageCodegen {
  val PIPELINE_DURATION_METRIC = "duration"
}

/**
 * WholeStageCodegen compile a subtree of plans that support codegen together into single Java
 * function.
 *
 * Here is the call graph of to generate Java source (plan A support codegen, but plan B does not):
 *
 *   WholeStageCodegen       Plan A               FakeInput        Plan B
 * =========================================================================
 *
 * -> execute()
 *     |
 *  doExecute() --------->   upstreams() -------> upstreams() ------> execute()
 *     |
 *     +----------------->   produce()
 *                             |
 *                          doProduce()  -------> produce()
 *                                                   |
 *                                                doProduce()
 *                                                   |
 *                         doConsume() <--------- consume()
 *                             |
 *  doConsume()  <--------  consume()
 *
 * SparkPlan A should override doProduce() and doConsume().
 *
 * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
 * used to generated code for BoundReference.
 */
case class WholeStageCodegen(child: SparkPlan) extends UnaryNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override private[sql] lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegen.PIPELINE_DURATION_METRIC))

  /**
   * Generates code for this subtree.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, String) = {
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      /** Codegened pipeline for:
       * ${toCommentSafeString(child.treeString.trim)}
       */
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripExtraNewLines(source)
    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    CodeGenerator.compile(cleanedSource)
    (ctx, cleanedSource)
  }

  /*
Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

rang/sum:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
rang/sum codegen=false                 13017 / 13872         40.3          24.8       1.0X
rang/sum codegen=true                    1358 / 1450        385.9           2.6       9.6X

Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

rang/sum:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
rang/sum codegen=false                 12895 / 14447         40.7          24.6       1.0X
rang/sum codegen=true                     380 /  435       1380.0           0.7      33.9X

Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.3
Intel(R) Core(TM) i5-4260U CPU @ 1.40GHz

rang/sum:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
rang/sum codegen=true                     543 /  675        965.7           1.0       1.0X
   */

  class NvlGeneratorException(msg: String = null, cause: Throwable = null)
    extends java.lang.Exception(msg, cause) {}

  def bottomLevelSingleCol(sp: SparkPlan): Boolean = sp match {
    case Range(_, _, _, _, _) => true
    case BatchedDataSourceScan(output, _, _, _, _) => output.size == 1
    case _ => false
  }

  def genExpr(e: Expression, child: SparkPlan): String = e match {
    case Literal(v, d) => v.toString + (if (d.isInstanceOf[LongType]) "L" else "")
    case IsNotNull(_) => "true"
    case And(l, r) => "(" + genExpr(l, child) + ") && (" + genExpr(r, child) + ")"
    case Or(l, r) => "(" + genExpr(l, child) + ") || (" + genExpr(r, child) + ")"
    case Add(l, r) => "(" + genExpr(l, child) + ") + (" + genExpr(r, child) + ")"
    case Multiply(l, r) => "(" + genExpr(l, child) + ") * (" + genExpr(r, child) + ")"
    case Subtract(l, r) => "(" + genExpr(l, child) + ") - (" + genExpr(r, child) + ")"
    case BitwiseAnd(l, r) => "(" + genExpr(l, child) + ") & (" + genExpr(r, child) + ")"
    case EqualTo(l, r) => "(" + genExpr(l, child) + ") == (" + genExpr(r, child) + ")"
    case GreaterThan(l, r) => "(" + genExpr(l, child) + ") > (" + genExpr(r, child) + ")"
    case GreaterThanOrEqual(l, r) => "(" + genExpr(l, child) + ") >= (" + genExpr(r, child) + ")"
    case LessThan(l, r) => "(" + genExpr(l, child) + ") < (" + genExpr(r, child) + ")"
    case LessThanOrEqual(l, r) => "(" + genExpr(l, child) + ") <= (" + genExpr(r, child) + ")"
    case AttributeReference(n, _, _, _) => "data" + (if (bottomLevelSingleCol(child)) "" else "." + child.output.indexWhere(_.name == n))
    case Alias(c, _) => genExpr(c, child)
    case t => throw new NvlGeneratorException(t.getClass.toString)
  }

  def nvlType(d: DataType): String = d match {
    case IntegerType => "int"
    case LongType => "long"
    case FloatType => "float"
    case DoubleType => "double"
    case t => throw new NvlGeneratorException(t.getClass.toString)
  }

  def nvlZero(d: DataType): String = d match {
    case IntegerType => "0"
    case LongType => "0L"
    case FloatType => "0.0"
    case DoubleType => "0.0"
    case t => throw new NvlGeneratorException(t.getClass.toString)
  }

  def genNvlHelper(sp: SparkPlan, args: ArrayBuffer[(String, String)],
                   topLevel: Boolean): String = sp match {
    case Project(exprs, child) =>
      val childNvl = genNvlHelper(child, args, false)
      val genedExprs = exprs.map(genExpr(_, child))
      val outElTypes = exprs.map(e => nvlType(e.dataType))
      // TODO assuming we won't have more than 64 columns so only one null bitfield is needed ...
      val builderType = s"vecBuilder[{${if (topLevel) "long, " else ""}${outElTypes.mkString(", ")}}]"
      val inElTypes = child.output.map(e => nvlType(e.dataType))
      var dataType = inElTypes.mkString(", ")
      if (!bottomLevelSingleCol(child)) {
        dataType = "{" + dataType + "}"
      }
      val mergeValue = s"{${if (topLevel) "0L, " else ""}${genedExprs.mkString(", ")}}"
      s"""
         |$childNvl;
         |${if (topLevel) "" else "data := "}res(for(data,
         |    $builderType, (bld: $builderType, i: long,
         |    data: $dataType) =>
         |    merge(bld, $mergeValue)
         |   ))
       """.
        stripMargin
    case Filter(cond, child) =>
      val childBottomSingleCol = bottomLevelSingleCol(child)
      val childNvl = genNvlHelper(child, args, false)
      val genedCond = genExpr(cond, child)
      val inElTypes = child.output.map(e => nvlType(e.dataType))
      var dataType = inElTypes.mkString(", ")
      if (!childBottomSingleCol) {
        dataType = "{" + dataType + "}"
      }
      // TODO assuming we won't have more than 64 columns so only one null bitfield is needed ...
      val builderType = s"vecBuilder[{${if (topLevel) "long, " else ""}${inElTypes.mkString(", ")}}]"
      val toMerge = if (childBottomSingleCol) Seq("data") else (0 until child.output.size).map(i => s"data.$i")
      val mergeValue = s"{${if (topLevel) "0L, " else ""}${toMerge.mkString(", ")}}"
      s"""
         |$childNvl;
         |${if (topLevel) "" else "data := "}res(for(data,
         |    $builderType, (bld: $builderType, i: long,
         |    data: $dataType) =>
         |    if ($genedCond, merge(bld, $mergeValue), bld)
         |   ))
       """.
        stripMargin
    case TungstenAggregate(_, gExpr, aExpr, _, _, _, child) =>
      val childNvl = genNvlHelper(child, args, false)
      if (!topLevel) {
        throw new NvlGeneratorException("aggregation only allowed at top level")
      }
      val genedGExpr = gExpr.map(genExpr(_, child))
      val genedAExpr = aExpr.map(a => {
        a.aggregateFunction match {
          case Sum(c) => {
            val cType = nvlType(c.dataType)
            (cType, a.aggregateFunction, genExpr(c, child), nvlZero(c.dataType))
          }
          case _ => throw new NvlGeneratorException(a.aggregateFunction.getClass.toString)
        }
      })
      // TODO assuming we won't have more than 64 columns so only one null bitfield is needed ...
      var typeStruct = "{" + (if (genedGExpr.size == 0) "long, " else "") + genedAExpr.map(t => t._1).mkString(", ") + "}"
      var updateBody =
        "{" + (if (genedGExpr.size == 0) "0L, " else "") +
        genedAExpr.zipWithIndex.map(t => t._1._2 match {
          case Sum(_) =>
            val increment = if (genedGExpr.size == 0) 1 else 0
            s"a.${t._2 + increment} + b.${t._2 + increment}"
          case _ => ""
        }).mkString(", ") + "}"
      val updateFunction = s"(a: $typeStruct, b: $typeStruct) => $updateBody"
      val builderType =
        if (genedGExpr.size > 0) {
          s"dictMerger[{long, ${gExpr.map(e => nvlType(e.dataType)).mkString(", ")}}, $updateFunction]"
        } else {
          s"merger[{0L, ${genedAExpr.map(t => t._4).mkString(", ")}}, $updateFunction, $updateFunction]"
        }
      val inElTypes = child.output.map(e => nvlType(e.dataType))
      var dataType = inElTypes.mkString(", ")
      if (!bottomLevelSingleCol(child)) {
        dataType = "{" + dataType + "}"
      }
      val mergeValue =
        if (genedGExpr.size == 0)
          s"{0L, ${genedAExpr.map(t => t._3).mkString(", ")}}"
        else
          s"{{0L, ${genedGExpr.mkString(", ")}}, {${genedAExpr.map(t => t._3).mkString(", ")}}}"
      s"""
         |$childNvl;
         |${if (genedGExpr.size > 0) "toVec(" else ""}res(for(data,
         |    $builderType, (bld: $builderType, i: long,
         |    data: $dataType) =>
         |    merge(bld, $mergeValue)
         |   ))${if (genedGExpr.size > 0) ")" else ""}
       """.stripMargin
    case BatchedDataSourceScan(output, _, _, _, _) =>
      if (topLevel) {
        throw new NvlGeneratorException("don't use NVL for scan only")
      }
      for ((out, i) <- output.zipWithIndex) {
        args += (("$" + i, s"vec[${nvlType(out.dataType)}]"))
      }
      var ret = args.map(_._1).mkString(", ")
      if (output.size > 1) {
        ret = "zip(" + ret + ")"
      }
      s"data := $ret"
    case Range(s, 1, 1, n, _) =>
      if (topLevel) {
        throw new NvlGeneratorException("don't use NVL for range only")
      }
      s"data := range($s, ${s + n})"
    case t => throw new NvlGeneratorException(t.getClass.toString)
  }

  def genNvl(sp : SparkPlan, args : ArrayBuffer[(String, String)]) : String = {
    val baseNvl = genNvlHelper(sp, args, true)
    s"(${args.map(t => t._1 + ": " + t._2).mkString(", ")}) => $baseNvl"
  }

  override def doExecute(): RDD[InternalRow] = {
    val durationMs = longMetric("pipelineTime")

    val rdds = child.asInstanceOf[CodegenSupport].upstreams()
    assert(rdds.size <= 2, "Up to two upstream RDDs can be supported")

    /*
    val useNvl = child match {
      case TungstenAggregate(_, _, _, _, _, _, Range(s, 1, 1, n, output)) => true
      case _ => false
    }
    */

    /*
    val useNvl = child match {
      case Project(_, Range(s, 1, 1, n, output)) => true
      case _ => false
    }
    */

    /*
    val useNvl = child match {
      case Project(_, BatchedDataSourceScan(_, _, _, _, _)) => true
      case _ => false
    }
    */

    var useNvl = true
    var nvlStr : String = null
    val args = ArrayBuffer.empty[(String, String)]
    try {
      nvlStr = genNvl(child, args)
    } catch {
      case e : NvlGeneratorException => {
        println(e)
        useNvl = false
      }
    }
    println(nvlStr)
    val hasRange = if (nvlStr != null) nvlStr.contains("range") else false

    if (!useNvl) {
      val (ctx, cleanedSource) = doCodeGen()
      val references = ctx.references.toArray

      if (rdds.length == 1) {
        rdds.head.mapPartitionsWithIndex { (index, iter) =>
          val clazz = CodeGenerator.compile(cleanedSource)
          val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
          buffer.init(index, Array(iter))
          new Iterator[InternalRow] {
            override def hasNext: Boolean = {
              val v = buffer.hasNext
              if (!v) durationMs += buffer.durationMs()
              v
            }

            override def next: InternalRow = buffer.next()
          }
        }
      } else {
        // Right now, we support up to two upstreams.
        rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
          val partitionIndex = TaskContext.getPartitionId()
          val clazz = CodeGenerator.compile(cleanedSource)
          val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
          buffer.init(partitionIndex, Array(leftIter, rightIter))
          new Iterator[InternalRow] {
            override def hasNext: Boolean = {
              val v = buffer.hasNext
              if (!v) durationMs += buffer.durationMs()
              v
            }

            override def next: InternalRow = buffer.next()
          }
        }
      }
    } else {
/*      val code =
        """
          |() => agg(range(0, 524288000), 0L, (a: long, b: long) => a + b)
        """.stripMargin*/
      /*
      val code =
        """
          |() => map(range(0, 1000), (l: long) => {0L, l * 2L})
        """.stripMargin
      */

      // assume rdds.length == 1
      // sqlContext.sparkContext.hadoopConfiguration.setBoolean("parquet.enable.dictionary", false)
      // sqlContext.read.format("csv").option("inferSchema", "true").option("delimiter", "|").option("mode", "FAILFAST").load("/Users/joseph/tpch-perf/tpch/lineitem.tbl").write.parquet("tpch-sf1")
      // val df2 = df.withColumn("shipdate", df("C10").cast(StringType)).withColumn("quantity", df("C4").cast(LongType))
      // val toLong = udf[Long, String]( _.split(" ")(0).replace("-", "").toLong)
      // df2.withColumn("shipdate_long", toLong(df2("shipdate"))).select("shipdate_long", "C6", "quantity", "C5").write.parquet("tpch-sf1-q6")
      // sqlContext.read.parquet("tpch-sf1-q6-nodict").filter("shipdate_long >= 19940101 and shipdate_long < 19950101 and C6 >= 0.05 and C6 <= 0.07 and quantity < 24").selectExpr("sum(C5 * C6)").explain
      // val C8toLong = udf[Long, String]( s => if (s == "N") 0L else if (s == "R") 1L else 2L)
      // val C9toLong = udf[Long, String]( s => if (s == "O") 0L else 1L)
      // sqlContext.read.parquet("tpch-sf1-q1").filter("shipdate_long <= 19981111").selectExpr("quantity", "C5", "C6", "C5 * (1 - C6) as a", "C5 * (1 - C6) * (1 + C7) as b", "returnflag", "linestatus").groupBy("returnflag", "linestatus").sum("quantity", "C5", "C6", "a", "b")
      rdds.head.mapPartitionsWithIndex { (index, iter) =>
        new Iterator[InternalRow] {
          var index = 0
          val row = new UnsafeRow(1)
          var lastResult : (Long, Long, Int) = (0, 0, 0)
          var curPtr = lastResult._1
          // TODO hack for range
          var firstNext = true
          val nvlCode = LlvmCompiler.compile(new NvlParser().parseFunction(nvlStr), 1, None, None)

          override def hasNext: Boolean = {
            /*
            println("START")
            println(iter.hasNext)
            println(iter.getClass.toString)
            println(index)
            println(lastResult._2)
            println("END")
            */
            if (!hasRange) {
              index < lastResult._2 || iter.hasNext
            } else {
              firstNext || index < lastResult._2
            }
          }

          override def next: InternalRow = {
            firstNext = false
            var useWriter = false
            var writer : UnsafeRowWriter = null
            // println("NEXT CALLED")
            if (index >= lastResult._2) {
              val nvlArgs =
                if (args.size > 0) {
                  val cb = iter.next().asInstanceOf[ColumnarBatch]
                  (0 until args.size).map(i => {
                    // val cb = n.asInstanceOf[ColumnarBatch]
                    val cv = cb.column(i).asInstanceOf[OffHeapColumnVector]
                    (cv.valuesNativeAddress(), cb.numRows().toLong)
                  })
                } else {
                  Seq.empty[(Long, Long)]
                }
              lastResult = nvlCode.run(if (nvlArgs.size == 1) nvlArgs(0) else nvlArgs).asInstanceOf[(Long, Long, Int)]
              // println("NEW RESULT:")
              // println(lastResult)
              if (lastResult._2 == -1) {
                useWriter = true
                writer = new UnsafeRowWriter(new BufferHolder(row, 0), 1)
              }
              index = 0
              curPtr = lastResult._1
            }
            index += 1
            if (useWriter) {
              writer.write(0, lastResult._1)
            } else {
              row.pointTo(null, curPtr, lastResult._3)
            }
            curPtr += lastResult._3
            row
          }
        }
      }
      /*
      rdds.head.mapPartitionsWithIndex { (idx, iter) =>
        new Iterator[InternalRow] {
          var index = 0
          val row = new UnsafeRow(1)
          val hasBatches = iter.hasNext
          val batch = if (hasBatches) iter.next().asInstanceOf[ColumnarBatch] else null
          val column = if (hasBatches) batch.column(0).asInstanceOf[OffHeapColumnVector] else null
          val code =
            """
              |(v: vec[long]) => map(v, (l: long) => {0L, l * 2L})
            """.stripMargin
          var curPtr = if (hasBatches) LlvmCompiler.compile(new NvlParser().parseFunction(code), 1, None, None).run(column.valuesNativeAddress())
            .asInstanceOf[(Long, Long, Int)]._1 else 0
          println("ran!")
          // val holder = new BufferHolder(row, 0)
          // val writer = new UnsafeRowWriter(holder, 1)
          override def hasNext: Boolean = {
            hasBatches && index < 1000
          }

          override def next: InternalRow = {
            index += 1
            // writer.write(0, result)
            row.pointTo(null, curPtr, 16)
            curPtr += 16
            row
          }
        }
      }
      */
    }
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val doCopy = if (ctx.copyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
      |${row.code}
      |append(${row.value}$doCopy);
     """.stripMargin.trim
  }

  override def innerChildren: Seq[SparkPlan] = {
    child :: Nil
  }

  private def collectInputs(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case InputAdapter(c) => c :: Nil
    case other => other.children.flatMap(collectInputs)
  }

  override def treeChildren: Seq[SparkPlan] = {
    collectInputs(child)
  }

  override def simpleString: String = "WholeStageCodegen"
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 */
case class CollapseCodegenStages(conf: SQLConf) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    case e: CaseWhen => e.shouldCodegen
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case u: UserDefinedType[_] => numOfNestedFields(u.sqlType)
    case _ => 1
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      val haveTooManyFields = numOfNestedFields(plan.schema) > conf.wholeStageMaxNumFields
      !willFallback && !haveTooManyFields
    case _ => false
  }

  /**
   * Inserts a InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = plan match {
    case j @ SortMergeJoin(_, _, _, _, left, right) if j.supportCodegen =>
      // The children of SortMergeJoin should do codegen separately.
      j.copy(left = InputAdapter(insertWholeStageCodegen(left)),
        right = InputAdapter(insertWholeStageCodegen(right)))
    case p if !supportCodegen(p) =>
      // collapse them recursively
      InputAdapter(insertWholeStageCodegen(p))
    case p =>
      p.withNewChildren(p.children.map(insertInputAdapter))
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
    case plan: CodegenSupport if supportCodegen(plan) =>
      WholeStageCodegen(insertInputAdapter(plan))
    case other =>
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled) {
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
