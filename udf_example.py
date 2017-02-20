import time
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from nvludfs import *

@nvl("(long,long,long)->long", tupargs=True)
def dot_prod(a,b,c):
  return 3*a+2*b+c

SparkContext.setSystemProperty("useNvl", "true")
SparkContext.setSystemProperty("offHeap", "true")
SparkContext.setSystemProperty("pythonNvl", "true")
conf = (SparkConf()
         .setMaster("local")
         .setAppName("udf_example")
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
dot_udf = udf(dot_prod, LongType())
df = sqlContext.read.parquet("assembly/udf-test-s").cache()
times = []
for i in range(0, 11):
  t = time.time() 
  df.withColumn("udf", dot_udf(df['a'], df['b'], df['c'])).selectExpr("sum(udf)").show()
  times.append(time.time() - t)
print "average time: " + str(sum(times[1:])/10.0)
