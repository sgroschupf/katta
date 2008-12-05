/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.testutil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Writer;

public class HadoopTestUtil {

  public static void writeSequenceFile(Configuration conf, Path filePath, WritableComparable... keys)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Writer writer = SequenceFile.createWriter(fs, conf, filePath, keys[0].getClass(), NullWritable.class);
    for (WritableComparable keyWritable : keys) {
      writer.append(keyWritable, NullWritable.get());
    }
    writer.close();
  }

  public static void writeSequenceFile(Configuration conf, Path filePath, TestDocumentRecord... documentRecords)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Writer writer = SequenceFile.createWriter(fs, conf, filePath, documentRecords[0].getKey().getClass(),
        MapWritable.class);
    for (TestDocumentRecord documentRecord : documentRecords) {
      writer.append(documentRecord.getKey(), documentRecord.getFieldMap());
    }
    writer.close();
  }

  public static void writeSequenceFile(Configuration conf, Path filePath,
      ITestDocumentRecordGenerator documentGenerator, int docCount) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Writer writer = SequenceFile.createWriter(fs, conf, filePath, documentGenerator.generate(0).getKey().getClass(),
        MapWritable.class);
    for (int i = 0; i < docCount; i++) {
      TestDocumentRecord documentRecord = documentGenerator.generate(i);
      writer.append(documentRecord.getKey(), documentRecord.getFieldMap());
    }
    writer.close();
  }

  public static interface ITestDocumentRecordGenerator {
    TestDocumentRecord generate(int i);
  }

  public static class TestDocumentRecord {

    private final WritableComparable _key;
    private final MapWritable _fieldMap;

    public TestDocumentRecord(WritableComparable key, MapWritable fieldMap) {
      _key = key;
      _fieldMap = fieldMap;
    }

    public WritableComparable getKey() {
      return _key;
    }

    public MapWritable getFieldMap() {
      return _fieldMap;
    }

  }
}
