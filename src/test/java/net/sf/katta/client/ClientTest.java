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

import java.util.Set;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.master.Master;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Test for {@link Client}.
 */
public class ClientTest extends AbstractKattaTest {

  private static Logger LOG = Logger.getLogger(ClientTest.class);

  private static final String INDEX1 = "index1";
  private static final String INDEX2 = "index2";
  private static final String INDEX3 = "index3";

  private static Node _node1;
  private static Node _node2;
  private static Master _master;
  private static IDeployClient _deployClient;
  private static IClient _client;

  public ClientTest() {
    super(false);
  }

  @Override
  protected void onBeforeClass() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    _master = masterStartThread.getMaster();

    NodeStartThread nodeStartThread1 = startNode();
    NodeStartThread nodeStartThread2 = startNode();
    _node1 = nodeStartThread1.getNode();
    _node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitOnNodes(masterStartThread, 2);

    _deployClient = new DeployClient(_conf);
    _deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1)
        .joinDeployment();
    _deployClient.addIndex(INDEX2, TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1)
        .joinDeployment();
    _deployClient.addIndex(INDEX3, TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1)
        .joinDeployment();
    _client = new Client();
  }

  @Override
  protected void onAfterClass() throws Exception {
    _client.close();
    _deployClient.disconnect();
    _node1.shutdown();
    _node2.shutdown();
    _master.shutdown();
  }

  public void testCount() throws KattaException {
    final Query query = new Query("content: the");
    final int count = _client.count(query, new String[] { INDEX1 });
    assertEquals(937, count);
  }

  public void testGetDetails() throws KattaException {
    final Query query = new Query("content:the");
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

  public void testSearch() throws KattaException {
    final Query query = new Query("foo: bar");
    float currentQueryPerMinute = _client.getQueryPerMinute();
    final Hits hits = _client.search(query, new String[] { INDEX3, INDEX2 });
    assertNotNull(hits);
    assertEquals(currentQueryPerMinute + 1, _client.getQueryPerMinute());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    assertEquals(8, hits.size());
    assertEquals(8, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  public void testSearchLimit() throws KattaException {
    final Query query = new Query("foo: bar");
    final Hits hits = _client.search(query, new String[] { INDEX3, INDEX2 }, 1);
    assertNotNull(hits);
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    assertEquals(8, hits.size());
    assertEquals(1, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  public void testSearchSimiliarity() throws KattaException {
    final Query query = new Query("foo: bar");
    final Hits hits = _client.search(query, new String[] { INDEX2 });
    assertNotNull(hits);
    assertEquals(4, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

}
