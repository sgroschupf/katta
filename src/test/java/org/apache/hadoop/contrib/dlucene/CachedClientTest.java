/**
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
package org.apache.hadoop.contrib.dlucene;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;

/**
 * JUnit test for CachedClient.
 */
public class CachedClientTest extends TestUtils {

  private static String[] CC_INDEX = { getNextIndex(), getNextIndex() };
  private static CachedClient cc = null;
  private static MiniDLuceneCluster cluster = null;

  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
    if (cluster == null) {
      cluster = new MiniDLuceneCluster(conf, 2);
      cc = new CachedClient(conf, new InetSocketAddress(Constants.HOST, cluster
          .getNameNodePort()));
      cc.createIndex(CC_INDEX[0], false);
      cc.createIndex(CC_INDEX[1], true);
    }
  }

  void waitForUpdate() {
    // wait for workers to send heartbeats to master
    long time = System.currentTimeMillis();
    Long failureInterval = Constants.HEARTBEAT_INTERVAL_VALUE * 3000;
    while (System.currentTimeMillis() < time + failureInterval) {
      // do nothing
    }
    // This is failing because the client is not updating the index
    cc.updateCache(true);
  }

  /**
   * Test CachedClient.getIndexes().
   */
  public void testGetIndexes() {
    Set<String> indexes = new HashSet<String>();
    for (String s : cc.getIndexes()) {
      indexes.add(s);
    }
    assertEquals(2, indexes.size());
    assertTrue(indexes.contains(CC_INDEX[0]));
    assertTrue(indexes.contains(CC_INDEX[1]));
  }

  /**
   * Test CachedClient.addDocument().
   * 
   * @throws Exception thrown by class under test
   */
  public void testAddDocument() throws Exception {
    Document doc = new Document();
    doc.add(exampleField);
    waitForUpdate();
    IIndexUpdater writer = cc.getIndexUpdater(CC_INDEX[0]);
    writer.addDocument(doc);
    writer.commit();
    SearchResults sr = cc.search(CC_INDEX[0], getQuery(FIELD_KEY,
        FORENAME[0]), new Sort(), 10);
    // check the query returns the original document
    assertEquals(1, sr.size());
    assertEquals(NAME[0], sr.get(0).get(FIELD_KEY));
  }

  /**
   * Test CachedClient.removeDocuments().
   * 
   * @throws Exception thrown by class under test
   */
  public void testRemoveDocuments() throws Exception {
    Document doc = new Document();
    Field field = new Field(FIELD_KEY, NAME[1], Field.Store.YES,
        Field.Index.TOKENIZED);
    doc.add(field);
    IIndexUpdater writer = cc.getIndexUpdater(CC_INDEX[0]);
    writer.addDocument(doc);
    writer.commit();
    SearchResults sr = cc.search(CC_INDEX[0], getQuery(FIELD_KEY,
        FORENAME[1]), new Sort(), 10);
    // check the query returns the original document
    assertEquals(1, sr.size());
    assertEquals(NAME[1], sr.get(0).get(FIELD_KEY));
    IIndexUpdater iu = cc.getIndexUpdater(CC_INDEX[0]);
    iu.removeDocuments(new Term(FIELD_KEY, FORENAME[1]));
    writer.commit();
    waitForUpdate();
    sr = cc.search(CC_INDEX[0], getQuery(FIELD_KEY, FORENAME[1]),
        new Sort(), 10);
    assertEquals(0, sr.size());
  }
}
