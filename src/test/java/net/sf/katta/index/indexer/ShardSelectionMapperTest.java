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

import java.io.DataOutput;
import java.io.IOException;

import junit.framework.TestCase;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class ShardSelectionMapperTest extends TestCase {

  public static class DummyKeyGenerator implements IShardKeyGenerator {

    public String getShardKey(final WritableComparable key, final Writable value, final Reporter reporter,
        final int ofShards) {
      return "foo";
    }
  }

  public void testSelect() throws IOException {

    final Mockery mockery = new Mockery();
    final Writable writable = mockery.mock(Writable.class);
    final WritableComparable writableComparable = mockery.mock(WritableComparable.class);
    final Reporter reporter = mockery.mock(Reporter.class);
    final OutputCollector outputCollector = mockery.mock(OutputCollector.class);

    mockery.checking(new Expectations() {
      {
        one(writableComparable).write(with(any(DataOutput.class)));
        one(writable).write(with(any(DataOutput.class)));
        one(outputCollector).collect(with(equal(new Text("foo"))), with(any(BytesWritable.class)));
      }
    });

    final Mapper mapper = new ShardSelectionMapper();
    final JobConf jobConf = new JobConf();
    jobConf.set(IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS, DummyKeyGenerator.class.getName());
    mapper.configure(jobConf);
    mapper.map(writableComparable, writable, outputCollector, reporter);

    mockery.assertIsSatisfied();

  }
}
