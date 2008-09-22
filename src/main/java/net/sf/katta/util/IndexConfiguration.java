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
package net.sf.katta.util;

import java.util.Set;

import net.sf.katta.index.indexer.Indexer;
import net.sf.katta.index.indexer.ShardSelectionMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

public class IndexConfiguration extends KattaConfiguration {

  public IndexConfiguration() {
    super("/katta.index.properties");
  }

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

  public JobConf createJobConf(final Configuration configuration) {
    JobConf jobConf = new JobConf(configuration);
    jobConf.setJobName("Index Xml");
    final Set<Object> keySet = _properties.keySet();
    for (final Object key : keySet) {
      final String value = getProperty((String) key);
      if (key.equals(MAPRED_INPUT_PATH)) {
        jobConf.setInputPath(new Path(value));
      } else if (key.equals(MAPRED_OUTPUT_PATH)) {
        jobConf.setOutputPath(new Path(value));
      } else if (key.equals(INPUT_FORMAT_CLASS)) {
        jobConf.setInputFormat((Class<? extends InputFormat>) getClass(key.toString()));
      } else {
        jobConf.set(key.toString(), value);
      }
    }

    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    jobConf.setMapperClass(ShardSelectionMapper.class);
    jobConf.setReducerClass(Indexer.class);
    return jobConf;
  }

  public void enrichJobConf(Configuration conf, String confKey) {
    conf.set(confKey, getProperty(confKey));
  }

}
