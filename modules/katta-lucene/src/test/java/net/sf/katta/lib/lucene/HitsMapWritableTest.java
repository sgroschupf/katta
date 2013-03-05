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
package net.sf.katta.lib.lucene;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.sf.katta.AbstractWritableTest;

import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

public class HitsMapWritableTest extends AbstractWritableTest {

  @Test
  public void testSerialization() throws Exception {
    String nodeName = "node1";
    HitsMapWritable hitsMapWritable = new HitsMapWritable(nodeName);
    String shard1 = "shard1";
    String shard2 = "shard2";
    String shard3 = "shard3";
    for (int i = 0; i < 200; i++) {
      hitsMapWritable.addHit(new Hit(shard1, nodeName, i, 10000 + i));
      hitsMapWritable.addHit(new Hit(shard2, nodeName, i, 20000 + i));
      hitsMapWritable.addHit(new Hit(shard3, nodeName, i, 30000 + i));
    }

    DataOutputBuffer out = writeWritable(hitsMapWritable);
    HitsMapWritable readWritable = (HitsMapWritable) readWritable(out, new HitsMapWritable());

    assertEquals(hitsMapWritable.getNodeName(), readWritable.getNodeName());
    assertEquals(hitsMapWritable.getTotalHits(), readWritable.getTotalHits());
    List<Hit> hits = hitsMapWritable.getHitList();
    List<Hit> readHits = readWritable.getHitList();
    assertEquals(hits.size(), readHits.size());
    for (int i = 0; i < hits.size(); i++) {
      assertEquals(hits.get(i), readHits.get(i));
    }
    assertEquals(hitsMapWritable.getNodeName(), readWritable.getNodeName());
  }
}
