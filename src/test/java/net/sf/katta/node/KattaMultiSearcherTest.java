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
package net.sf.katta.node;

import junit.framework.Assert;
import junit.framework.TestCase;
import net.sf.katta.node.KattaMultiSearcher.KattaHitQueue;

public class KattaMultiSearcherTest extends TestCase {

  public void testPriorityQueue() throws Exception {
    // tests some simple PriorityQueue behaviro
    KattaHitQueue queue = new KattaMultiSearcher("node").new KattaHitQueue(2);
    Assert.assertTrue(queue.insert(new Hit("sahrd", "node", 1f, 1)));
    Assert.assertTrue(queue.insert(new Hit("sahrd", "node", 2f, 1)));
    Assert.assertTrue(queue.insert(new Hit("sahrd", "node", 3f, 1)));
    Assert.assertTrue(queue.insert(new Hit("sahrd", "node", 4f, 1)));
    Assert.assertEquals(2, queue.size());
  }

}
