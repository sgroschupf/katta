/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.index.indexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;


public class IndexJobConf extends JobConf {

  /**
   * Katta Related *
   */
  public static final String INDEX_ZIP_CLASS = "index.zip.class";


  public static final String DOCUMENT_FACTORY_CLASS = "document.factory.class";

  public static final String INDEX_PUBLISHER_CLASS = "index.publisher.class";

  public static final String INDEX_SHARD_KEY_GENERATOR_CLASS = "indexer.shardKeyGenerator.class";

  public static final String INDEX_SHARD_COUNT = "index.shardCount";

  /**
   * Lucene Related *
   */
  public static final String FLUSH_THRESHOLD = "index.flush.threshold";

  public static final String INDEXER_MERGE_FACTOR = "indexer.mergeFactor";

  public static final String INDEXER_MAX_MERGE = "indexer.maxMergeDocs";

  public static final String INDEXER_TERM_INTERVALL = "indexer.term.intervall";

  public static final String INDEXER_MAX_FIELD_LENGTH = "indexer.max.fieldLength";

  public static final String INDEXER_MAX_BUFFERED_DOCS = "indexer.max.bufferedDocs";

  /**
   * Path Related *
   */
  public static final String INDEX_TMP_DIRECTORY = "index.tmp.directory";

  public static final String MAPRED_INPUT_PATH = "mapred.input.path";

  public static final String MAPRED_OUTPUT_PATH = "mapred.output.path";

  public static final String INDEX_UPLOAD_PATH = "index.upload.path";

  public static final String INDEX_ARCHIVE_PATH = "index.archive.path";

  /**
   * Mapred IO *
   */
  public static final String INPUT_FORMAT_CLASS = "input.format.class";

  public static final String INPUT_KEY_CLASS = "index.input.key.class";

  public static final String INPUT_VALUE_CLASS = "index.input.value.class";

  public JobConf create(final InputStream inputStream) throws Exception {
    return create(null, inputStream);
  }

  /**
   *
   * @param configuration
   * @param inputStream
   * @return a new created JobConf with settet mapOutputKey(Value) and Mapper,Reducer. No jobjar is settet. 
   * @throws Exception
   */
  public JobConf create(final Configuration configuration, final InputStream inputStream) throws Exception {
    JobConf jobConf;
    if (configuration != null) {
      jobConf = new JobConf(configuration);
    } else {
      jobConf = new JobConf();
    }

    jobConf.setJobName("Index Xml");
    final Properties properties = new Properties();
    properties.load(inputStream);

    final Set<Object> keySet = properties.keySet();
    for (final Object key : keySet) {
      final Object value = properties.get(key);
      if (key.toString().equals(MAPRED_INPUT_PATH)) {
        jobConf.setInputPath(new Path(value.toString()));
      } else if (key.toString().equals(MAPRED_OUTPUT_PATH)) {
        jobConf.setOutputPath(new Path(value.toString()));
      } else if (key.toString().equals(INPUT_FORMAT_CLASS)) {
        jobConf.setInputFormat((Class<? extends InputFormat>) jobConf.getClassByName(value.toString()));
      } else {
        jobConf.set(key.toString(), value.toString());
      }
    }
    inputStream.close();

    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    jobConf.setMapperClass(ShardSelectionMapper.class);
    jobConf.setReducerClass(Indexer.class);
    return jobConf;

  }

  public JobConf create(final File file) throws Exception {
    return create(new FileInputStream(file));
  }

  public JobConf create(final Configuration configuration, final File file) throws Exception {
    return create(configuration, new FileInputStream(file));
  }
}
