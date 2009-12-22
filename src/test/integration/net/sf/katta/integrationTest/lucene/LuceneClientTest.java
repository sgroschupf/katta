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
package net.sf.katta.integrationTest.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Set;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.IndexState;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link LuceneClient}.
 */
public class LuceneClientTest extends AbstractIntegrationTest {

  private static Logger LOG = Logger.getLogger(LuceneClientTest.class);

  private static final String INDEX1 = "index1";
  private static final String INDEX2 = "index2";
  private static final String INDEX3 = "index3";
  @Rule
  public TemporaryFolder _temporaryFolder = new TemporaryFolder();

  public LuceneClientTest() {
    super(2, false);
  }

  @Before
  public void setupClient() {
    // TODO Auto-generated method stub

  }

  @Test
  public void testInstantiateClientBeforeIndex() throws Exception {
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    deployTestIndices(1, getNodeCount());
    List<Node> nodes = _miniCluster.getNodes();
    for (Node node : nodes) {
      TestUtil.waitUntilNodeServesShards(_protocol, node.getName(), SHARD_COUNT);
    }

    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    client.count(query, new String[] { INDEX_NAME });
    client.close();
  }

  @Test
  public void testCount() throws Exception {
    deployTestIndices(1, 1);
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final int count = client.count(query, new String[] { INDEX_NAME });
    assertEquals(937, count);
    client.close();
  }

  @Test
  public void testGetDetails() throws Exception {
    deployTestIndices(1, 1);
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final Hits hits = client.search(query, new String[] { INDEX_NAME }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      final MapWritable details = client.getDetails(hit);
      final Set<Writable> keySet = details.keySet();
      assertFalse(keySet.isEmpty());
      final Writable writable = details.get(new Text("path"));
      assertNotNull(writable);
    }
    client.close();
  }

  @Test
  public void testGetDetailsConcurrently() throws KattaException, ParseException, InterruptedException {
    deployTestIndices(1, 1);
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("content: the");
    final Hits hits = client.search(query, new String[] { INDEX_NAME }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());
    List<MapWritable> detailList = client.getDetails(hits.getHits());
    assertEquals(hits.getHits().size(), detailList.size());
    for (int i = 0; i < detailList.size(); i++) {
      final MapWritable details1 = client.getDetails(hits.getHits().get(i));
      final MapWritable details2 = detailList.get(i);
      assertEquals(details1.entrySet(), details2.entrySet());
      final Set<Writable> keySet = details2.keySet();
      assertFalse(keySet.isEmpty());
      final Writable writable = details2.get(new Text("path"));
      assertNotNull(writable);
    }
  }

  @Test
  public void testSearch() throws Exception {
    deploy3Indices();
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = client.search(query, new String[] { INDEX3, INDEX2 });
    assertNotNull(hits);
    for (final Hit hit : hits.getHits()) {
      writeToLog(hit);
    }
    assertEquals(8, hits.size());
    assertEquals(8, hits.getHits().size());
    client.close();
  }

  @Test
  public void testSortedSearch() throws Exception {
    // write and deploy test index
    File sortIndex = _temporaryFolder.newFolder("sortIndex");
    String queryTerm = "2";
    String sortFieldName = "sortField";
    String textFieldName = "textField";
    IndexWriter indexWriter = new IndexWriter(sortIndex, new StandardAnalyzer(), true, MaxFieldLength.UNLIMITED);
    for (int i = 0; i < 20; i++) {
      Document document = new Document();
      document.add(new Field(sortFieldName, "" + i, Store.NO, Index.NOT_ANALYZED));
      String textField = "sample text";
      if (i % 2 == 0) {// produce some different scores
        for (int j = 0; j < i; j++) {
          textField += " " + queryTerm;
        }
      }
      document.add(new Field(textFieldName, textField, Store.NO, Index.ANALYZED));
      indexWriter.addDocument(document);
    }
    indexWriter.optimize();
    indexWriter.close();
    DeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    IndexState indexState = deployClient.addIndex(sortIndex.getName(), sortIndex.getParentFile().getAbsolutePath(), 1)
            .joinDeployment();
    assertEquals(IndexState.DEPLOYED, indexState);

    // query and compare results
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse(textFieldName + ": " + queryTerm);
    Sort sort = new Sort(new String[] { "sortField" });
    final Hits hits = client.search(query, new String[] { sortIndex.getName() }, 20, sort);
    assertNotNull(hits);
    List<Hit> hitsList = hits.getHits();
    for (final Hit hit : hitsList) {
      writeToLog(hit);
    }
    assertEquals(9, hits.size());
    assertEquals(9, hitsList.size());
    assertTrue("results ordered after score", hitsList.get(0).getScore() < hitsList.get(1).getScore());
    for (int i = 0; i < hitsList.size() - 1; i++) {
      int compareTo = hitsList.get(i).getSortFields()[0].compareTo(hitsList.get(i + 1).getSortFields()[0]);
      assertTrue("results not after field", compareTo == 0 || compareTo == -1);
    }
    client.close();
  }

  @Test
  public void testSearchLimit() throws Exception {
    deploy3Indices();
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = client.search(query, new String[] { INDEX3, INDEX2 }, 1);
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

  @Test
  public void testKatta20SearchLimitMaxNumberOfHits() throws Exception {
    deployTestIndices(1, getNodeCount());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Hits expectedHits = client.search(query, new String[] { INDEX_NAME }, 4);
    assertNotNull(expectedHits);
    LOG.info("Expected hits:");
    for (final Hit hit : expectedHits.getHits()) {
      writeToLog(hit);
    }
    assertEquals(4, expectedHits.getHits().size());

    for (int i = 0; i < 100; i++) {
      // Now we redo the search, but limit the max number of hits. We expect the
      // same ordering of hits.
      for (int maxHits = 1; maxHits < expectedHits.size() + 1; maxHits++) {
        final Hits hits = client.search(query, new String[] { INDEX_NAME }, maxHits);
        assertNotNull(hits);
        assertEquals(maxHits, hits.getHits().size());
        for (int j = 0; j < hits.getHits().size(); j++) {
          // writeToLog("expected: ", expectedHits.getHits().get(j));
          // writeToLog("actual : ", hits.getHits().get(j));
          assertEquals(expectedHits.getHits().get(j).getScore(), hits.getHits().get(j).getScore(), 0.0);
        }
      }
    }
  }

  @Test
  public void testSearchSimiliarity() throws Exception {
    deploy3Indices();
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    final Hits hits = client.search(query, new String[] { INDEX2 });
    assertNotNull(hits);
    assertEquals(4, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
  }

  @Test
  public void testNonExistentShard() throws Exception {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo: bar");
    ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    try {
      client.search(query, new String[] { "doesNotExist" });
      fail("Should have failed.");
    } catch (KattaException e) {
      assertEquals("No shards for indices: [doesNotExist]", e.getMessage());
    }
  }

  private void writeToLog(Hit hit) {
    LOG.info(hit.getNode() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
  }

  private void deploy3Indices() throws Exception {
    DeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(), 1).joinDeployment();
    deployClient.addIndex(INDEX2, TestResources.INDEX1.getAbsolutePath(), 1).joinDeployment();
    deployClient.addIndex(INDEX3, TestResources.INDEX1.getAbsolutePath(), 1).joinDeployment();
  }

}
