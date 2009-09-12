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
package net.sf.katta.client;

import java.util.List;
import java.util.Set;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.master.Master;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

/**
 * Test for {@link LuceneClient}.
 */
public class LuceneClientTest extends AbstractKattaTest {

  private static Logger LOG = Logger.getLogger(LuceneClientTest.class);

  private static final String INDEX1 = "index1";
  private static final String INDEX2 = "index2";
  private static final String INDEX3 = "index3";

  private static Node _node1;
  private static Node _node2;
  private static Master _master;
  private static IDeployClient _deployClient;
  private static ILuceneClient _client;

  public LuceneClientTest() {
    super(false);
  }

  @Override
  protected void onBeforeClass() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    _master = masterStartThread.getMaster();

    NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    NodeStartThread nodeStartThread2 = startNode(new LuceneServer());
    _node1 = nodeStartThread1.getNode();
    _node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitOnNodes(masterStartThread, 2);

    _deployClient = new DeployClient(masterStartThread.getZkClient(), _conf);
    _deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(), 1)
        .joinDeployment();
    _deployClient.addIndex(INDEX2, TestResources.INDEX1.getAbsolutePath(), 1)
        .joinDeployment();
    _deployClient.addIndex(INDEX3, TestResources.INDEX1.getAbsolutePath(), 1)
        .joinDeployment();
    _client = new LuceneClient(new DefaultNodeSelectionPolicy(), _conf);
  }

  @Override
  protected void onAfterClass() throws Exception {
    _client.close();
    _deployClient.disconnect();
    _node1.shutdown();
    _node2.shutdown();
    _master.shutdown();
  }

  public void testCount() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final int count = _client.count(query, new String[] { INDEX1 });
    assertEquals(937, count);
  }

  public void testGetDetails() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final Hits hits = _client.search(query, new String[] { INDEX1 }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      final MapWritable details = _client.getDetails(hit);
      final Set<Writable> keySet = details.keySet();
      assertFalse(keySet.isEmpty());
      final Writable writable = details.get(new Text("path"));
      assertNotNull(writable);
    }
  }

  public void testGetDetailsConcurrently() throws KattaException, ParseException, InterruptedException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final Hits hits = _client.search(query, new String[] { INDEX1 }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());
    List<MapWritable> detailList = _client.getDetails(hits.getHits());
    assertEquals(hits.getHits().size(), detailList.size());
    for (int i = 0; i < detailList.size(); i++) {
      final MapWritable details1 = _client.getDetails(hits.getHits().get(i));
      final MapWritable details2 = detailList.get(i);
      assertEquals(details1.entrySet(), details2.entrySet());
      final Set<Writable> keySet = details2.keySet();
      assertFalse(keySet.isEmpty());
      final Writable writable = details2.get(new Text("path"));
      assertNotNull(writable);
    }
  }

  public void testSearch() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = _client.search(query, new String[] { INDEX3, INDEX2 });
    assertNotNull(hits);
    for (final Hit hit : hits.getHits()) {
      writeToLog(hit);
    }
    assertEquals(8, hits.size());
    assertEquals(8, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  public void testSearchLimit() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = _client.search(query, new String[] { INDEX3, INDEX2 }, 1);
    assertNotNull(hits);
    for (final Hit hit : hits.getHits()) {
      writeToLog(hit);
    }
    assertEquals(8, hits.size());
    assertEquals(1, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  public void testKatta20SearchLimitMaxNumberOfHits() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits expectedHits = _client.search(query, new String[] { INDEX1 }, 4);
    assertNotNull(expectedHits);
    LOG.info("Expected hits:");
    for (final Hit hit : expectedHits.getHits()) {
      writeToLog(hit);
    }
    assertEquals(4, expectedHits.getHits().size());

    for (int i = 0; i < 100; i++) {
      // Now we redo the search, but limit the max number of hits. We expect the same
      // ordering of hits.
      for (int maxHits = 1; maxHits < expectedHits.size() + 1; maxHits++) {
        final Hits hits = _client.search(query, new String[] { INDEX1 }, maxHits);
        assertNotNull(hits);
        assertEquals(maxHits, hits.getHits().size());
        for (int j = 0; j < hits.getHits().size(); j++) {
//           writeToLog("expected: ", expectedHits.getHits().get(j));
//           writeToLog("actual : ", hits.getHits().get(j));
          assertEquals(expectedHits.getHits().get(j).getScore(), hits.getHits().get(j).getScore());
        }
      }
    }
  }

  private void writeToLog(Hit hit) {
    LOG.info(hit.getNode() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
  }

  public void testSearchSimiliarity() throws KattaException, ParseException {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = _client.search(query, new String[] { INDEX2 });
    assertNotNull(hits);
    assertEquals(4, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  public void testNonExistentShard() throws Exception {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    try {
      _client.search(query, new String[] { "doesNotExist" });
      fail("Should have failed.");
    } catch (KattaException e) {
      assertEquals("No shards for indices: [doesNotExist]", e.getMessage());
    }
  }


}
