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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.ValuesReaderIntIterator;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.createRLEIterator;

/**
 * Decoder to return values from a single column.
 */
public class VectorizedColumnReader {
  /**
   * Total number of values read.
   */
  private long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean useDictionary;

  /**
   * Maximum definition level for this column.
   */
  private final int maxDefLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private SpecificParquetRecordReaderBase.IntIterator repetitionLevelColumn;
  private SpecificParquetRecordReaderBase.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;

  public VectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
      throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.useDictionary = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.useDictionary = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }

  /**
   * TODO: Hoist the useDictionary branch to decode*Batch and make the batch page aligned.
   */
  public boolean nextBoolean() {
    if (!useDictionary) {
      return dataColumn.readBoolean();
    } else {
      return dictionary.decodeToBoolean(dataColumn.readValueDictionaryId());
    }
  }

  public int nextInt() {
    if (!useDictionary) {
      return dataColumn.readInteger();
    } else {
      return dictionary.decodeToInt(dataColumn.readValueDictionaryId());
    }
  }

  public long nextLong() {
    if (!useDictionary) {
      return dataColumn.readLong();
    } else {
      return dictionary.decodeToLong(dataColumn.readValueDictionaryId());
    }
  }

  public float nextFloat() {
    if (!useDictionary) {
      return dataColumn.readFloat();
    } else {
      return dictionary.decodeToFloat(dataColumn.readValueDictionaryId());
    }
  }

  public double nextDouble() {
    if (!useDictionary) {
      return dataColumn.readDouble();
    } else {
      return dictionary.decodeToDouble(dataColumn.readValueDictionaryId());
    }
  }

  public Binary nextBinary() {
    if (!useDictionary) {
      return dataColumn.readBytes();
    } else {
      return dictionary.decodeToBinary(dataColumn.readValueDictionaryId());
    }
  }

  /**
   * Advances to the next value. Returns true if the value is non-null.
   */
  private boolean next() throws IOException {
    if (valuesRead >= endOfPageValueCount) {
      if (valuesRead >= totalValueCount) {
        // How do we get here? Throw end of stream exception?
        return false;
      }
      readPage();
    }
    ++valuesRead;
    // TODO: Don't read for flat schemas
    //repetitionLevel = repetitionLevelColumn.nextInt();
    return definitionLevelColumn.nextInt() == maxDefLevel;
  }

  /**
   * Reads `total` values from this columnReader into column.
   */
  void readBatch(int total, ColumnVector column) throws IOException {
    int rowId = 0;
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      if (useDictionary) {
        // Read and decode dictionary ids.
        ColumnVector dictionaryIds = column.reserveDictionaryIds(total);
        defColumn.readIntegers(
            num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        decodeDictionaryIds(rowId, num, column, dictionaryIds);
      } else {
        column.setDictionary(null);
        switch (descriptor.getType()) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(rowId, num, column, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }

      valuesRead += num;
      rowId += num;
      total -= num;
    }
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(int rowId, int num, ColumnVector column,
                                   ColumnVector dictionaryIds) {
    switch (descriptor.getType()) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BINARY:
        column.setDictionary(dictionary);
        break;
      case INT96:
        if (column.dataType() == DataTypes.TimestampType) {
          for (int i = rowId; i < rowId + num; ++i) {
            // TODO: Convert dictionary of Binaries to dictionary of Longs
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
            column.putLong(i, CatalystRowConverter.binaryToSQLTimestamp(v));
          }
        } else {
          throw new NotImplementedException();
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // DecimalType written in the legacy mode
        if (DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
            column.putInt(i, (int) CatalystRowConverter.binaryToUnscaledLong(v));
          }
        } else if (DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
            column.putLong(i, CatalystRowConverter.binaryToUnscaledLong(v));
          }
        } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
            column.putByteArray(i, v.getBytes());
          }
        } else {
          throw new NotImplementedException();
        }
        break;

      default:
        throw new NotImplementedException("Unsupported type: " + descriptor.getType());
    }
  }

  /**
   * For all the read*Batch functions, reads `num` values from this columnReader into column. It
   * is guaranteed that num is smaller than the number of values left in the current page.
   */

  private void readBooleanBatch(int rowId, int num, ColumnVector column) throws IOException {
    assert(column.dataType() == DataTypes.BooleanType);
    defColumn.readBooleans(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
        DecimalType.is32BitDecimalType(column.dataType())) {
      defColumn.readIntegers(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumn.readBytes(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumn.readShorts(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new NotImplementedException("Unimplemented type: " + column.dataType());
    }
  }

  private void readLongBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
        DecimalType.is64BitDecimalType(column.dataType())) {
      defColumn.readLongs(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readFloatBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumn.readFloats(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readDoubleBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumn.readDoubles(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new NotImplementedException("Unimplemented type: " + column.dataType());
    }
  }

  private void readBinaryBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.isArray()) {
      defColumn.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              // Read 12 bytes for INT96
              CatalystRowConverter.binaryToSQLTimestamp(data.readBinary(12)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new NotImplementedException("Unimplemented type: " + column.dataType());
    }
  }

  private void readFixedLenByteArrayBatch(int rowId, int num,
                                          ColumnVector column, int arrayLen) throws IOException {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (DecimalType.is32BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
              (int) CatalystRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              CatalystRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new NotImplementedException("Unimplemented type: " + column.dataType());
    }
  }

  private void readPage() throws IOException {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
            "could not read page in col " + descriptor +
                " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new NotImplementedException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.useDictionary = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new NotImplementedException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedPlainValuesReader();
      this.useDictionary = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new NotImplementedException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    dlReader = this.defColumn;
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
        page.getRepetitionLevels(), descriptor);

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumn);
    this.defColumn.initFromBuffer(
        this.pageValueCount, page.getDefinitionLevels().toByteArray());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
