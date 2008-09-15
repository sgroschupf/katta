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
package net.sf.katta.index.indexer;

import junit.framework.TestCase;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Reporter;
import org.jmock.Mockery;

public class DefaultShardKeyGeneratorTest extends TestCase {

  public void testGenerate() {
    final Mockery mockery = new Mockery();
    final Writable writable = mockery.mock(Writable.class);
    final WritableComparable writableComparable = mockery.mock(WritableComparable.class);
    final Reporter reporter = mockery.mock(Reporter.class);

    final IShardKeyGenerator generator = new DefaultShardKeyGenerator();

    for (int i = 0; i < 23; i++) {
      final String key = generator.getShardKey(writableComparable, writable, reporter, 10);
      assertEquals("" + i % 10, key);
    }

  }
}
