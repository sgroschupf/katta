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
package net.sf.katta;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.katta.client.DefaultNodeSelectionPolicy;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.master.Master;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;

import org.apache.hadoop.io.WritableComparable;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;

/**
 * Test common lucene operations on sharded indices through katta interface
 * versus pure lucene interface one big index.
 * 
 */
public class LuceneComplianceTest extends AbstractKattaTest {

  private static Node _node1;
  private static Node _node2;
  private static Master _master;
  private static IDeployClient _deployClient;
  private static ILuceneClient _client;

  // index related fields
  private static String FIELD_NAME = "text";
  private static File kattaIndex;
  private static File luceneIndex;
  private static List<Document> documents1;
  private static List<Document> documents2;

  public LuceneComplianceTest() {
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

    // generate 3 index (2 shards + once combined index)
    _deployClient = new DeployClient(masterStartThread.getZkClient(), _conf);
    kattaIndex = createClassWideFile("kattaIndex");
    File shard1 = new File(kattaIndex, "shard1");
    File shard2 = new File(kattaIndex, "shard2");
    luceneIndex = createClassWideFile("luceneIndex");
    documents1 = createSimpleNumberDocuments(FIELD_NAME, 123);
    documents2 = createSimpleNumberDocuments(FIELD_NAME, 78);

    writeIndex(shard1, documents1);
    writeIndex(shard2, documents2);
    writeIndex(luceneIndex, combineDocuments(documents1, documents2));

    // deploy 2 indexes to katta
    deployIndexToKatta(_deployClient, kattaIndex, 2);

    _client = new LuceneClient(new DefaultNodeSelectionPolicy(), _conf);
  }

  @Override
  protected void onAfterClass() throws Exception {
    _client.close();
    _node1.shutdown();
    _node2.shutdown();
    _master.shutdown();
  }

  public void testScoreSort() throws Exception {
    // query and compare
    IndexSearcher indexSearcher = new IndexSearcher(luceneIndex.getAbsolutePath());
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "0", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "1", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "2", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "15", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "23", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "2 23", null);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "nothing", null);
  }

  public void testFieldSort() throws Exception {
    // query and compare
    IndexSearcher indexSearcher = new IndexSearcher(luceneIndex.getAbsolutePath());
    Sort sort = new Sort(new SortField[] { new SortField(FIELD_NAME) });
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "0", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "1", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "2", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "15", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "23", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "2 23", sort);
    checkQueryResults(indexSearcher, kattaIndex.getName(), FIELD_NAME, "nothing", sort);
  }

  private void checkQueryResults(IndexSearcher indexSearcher, String kattaIndexName, String fieldName,
      String queryTerm, Sort sort) throws Exception {
    // check all documents
    checkQueryResults(indexSearcher, kattaIndexName, fieldName, queryTerm, Short.MAX_VALUE, sort);

    // check top n documents
    checkQueryResults(indexSearcher, kattaIndexName, fieldName, queryTerm, (documents1.size() + documents2.size()) / 2,
        sort);
  }

  private void checkQueryResults(IndexSearcher indexSearcher, String kattaIndexName, String fieldName,
      String queryTerm, int resultCount, Sort sort) throws Exception {
    final Query query = new QueryParser("", new KeywordAnalyzer()).parse(fieldName + ": " + queryTerm);
    final TopDocs searchResultsLucene;
    final Hits searchResultsKatta;
    if (sort == null) {
      searchResultsLucene = indexSearcher.search(query, resultCount);
      searchResultsKatta = _client.search(query, new String[] { kattaIndexName }, resultCount);
    } else {
      searchResultsLucene = indexSearcher.search(query, null, resultCount, sort);
      searchResultsKatta = _client.search(query, new String[] { kattaIndexName }, resultCount, sort);
    }

    assertEquals(searchResultsLucene.totalHits, searchResultsKatta.size());

    ScoreDoc[] scoreDocs = searchResultsLucene.scoreDocs;
    List<Hit> hits = searchResultsKatta.getHits();
    if (sort == null) {
      for (int i = 0; i < scoreDocs.length; i++) {
        assertEquals(scoreDocs[i].score, hits.get(i).getScore());
      }
    } else {
      System.err.println("------------------------");
      for (int i = 0; i < scoreDocs.length; i++) {
        Comparable[] luceneFields = ((FieldDoc) scoreDocs[i]).fields;
        WritableComparable[] kattaFields = hits.get(i).getSortFields();
        System.err.println(Arrays.asList(luceneFields) + " / " + Arrays.asList(kattaFields));
        assertEquals(luceneFields.length, kattaFields.length);
        for (int j = 0; j < luceneFields.length; j++) {
          assertEquals(luceneFields[j].toString(), kattaFields[j].toString());
        }

        // Arrays.equals(scoreDocs, kattaFields);
      }
    }
  }

  private List<Document> createSimpleNumberDocuments(String textFieldName, int count) {
    List<Document> documents = new ArrayList<Document>();
    for (int i = 0; i < count; i++) {
      String fieldContent = i + " " + (count - i);
      if (i % 2 == 0) {
        fieldContent += " 2";
      } else {
        fieldContent += " 1";
      }
      Document document = new Document();
      document.add(new Field(textFieldName, fieldContent, Store.NO, Index.ANALYZED));
      documents.add(document);
    }
    return documents;
  }

  private List<Document> combineDocuments(List<Document>... documentLists) {
    ArrayList<Document> list = new ArrayList<Document>();
    for (List<Document> documentsList : documentLists) {
      list.addAll(documentsList);
    }

    return list;
  }

  private void writeIndex(File file, List<Document> documents) throws IOException {
    IndexWriter indexWriter = new IndexWriter(file, new StandardAnalyzer(), true, MaxFieldLength.UNLIMITED);
    for (Document document : documents) {
      indexWriter.addDocument(document);
    }
    indexWriter.optimize();
    indexWriter.close();

  }

  private void deployIndexToKatta(IDeployClient deployClient, File file, int replicationLevel)
      throws InterruptedException {
    IndexState indexState = deployClient.addIndex(file.getName(), file.getAbsolutePath(), replicationLevel)
        .joinDeployment();
    assertEquals(IndexState.DEPLOYED, indexState);
    Thread.sleep(1000);// wait until lucene client is aware of the index
  }

}
