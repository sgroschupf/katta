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

import java.io.IOException;

import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ShardSelectionMapper implements Mapper {

  private final static Logger LOG = Logger.getLogger(ShardSelectionMapper.class);

  private IShardKeyGenerator _shardKeyGenerator;
  private int _numOfShards;
  private final DataOutputBuffer _buffer = new DataOutputBuffer();

  private final BytesWritable _bytesWritable = new BytesWritable();

  public void map(final WritableComparable key, final Writable value, final OutputCollector out, final Reporter reporter)
      throws IOException {
    final String shardKey = _shardKeyGenerator.getShardKey(key, value, reporter, _numOfShards);
    _buffer.reset();
    key.write(_buffer);
    value.write(_buffer);
    final byte[] bytes = _buffer.getData();
    _bytesWritable.set(bytes, 0, bytes.length);
    out.collect(new Text(shardKey), _bytesWritable);
  }

  public void configure(final JobConf jobconf) {
    final Class<?> generator = jobconf.getClass(IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS,
        IShardKeyGenerator.class);
    LOG.debug("use shardKeyGenerator '" + generator.getName() + "' to generate shard keys");
    try {
      _shardKeyGenerator = (IShardKeyGenerator) generator.newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("unable to instantiate shardKeyGenerator: ", e);
    }
    _numOfShards = jobconf.getInt(IndexConfiguration.INDEX_SHARD_COUNT, 10);
  }

  public void close() throws IOException {
    // nothing to do here...

  }

}
