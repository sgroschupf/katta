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

import java.io.IOException;
import java.net.InetSocketAddress;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.contrib.dlucene.writable.WDocument;
import org.apache.hadoop.contrib.dlucene.writable.WQuery;
import org.apache.hadoop.contrib.dlucene.writable.WSort;
import org.apache.hadoop.contrib.dlucene.writable.WTerm;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;

public class DataNodeTest extends TestUtils {

  private static MiniDLuceneCluster cluster = null;
  private static DataNode dn = null;
  private static int dataNodePort;
  private static String DNT_INDEX_ONE = null;
  private static String DNT_INDEX_TWO = null;
  private static String DNT_INDEX_THREE = null;

  protected void setUp() throws Exception {
    super.setUp();
    if (cluster == null) {
      cluster = new MiniDLuceneCluster(conf, 2);
    }
    if (DNT_INDEX_ONE == null) {
      DNT_INDEX_ONE = getNextIndex();
      DNT_INDEX_TWO = getNextIndex();
      DNT_INDEX_THREE = getNextIndex();
      // delete the old indexes before starting up
      // deleteDirectory(new File(ROOT_DIR_STR));
      dataNodePort = getNextPort();
      final ZKClient zkclient = new ZKClient(new ZkConfiguration());
      dn = DataNode.createNode(zkclient, conf, new InetSocketAddress(Constants.HOST,
          dataNodePort), new InetSocketAddress(Constants.HOST, cluster
          .getNameNodePort()), USE_RAM_INDEX_FOR_TESTS);
      dn.createIndex(DNT_INDEX_ONE);
    }
  }

  public void testNullArguments() throws Exception {
    try {
      dn.addDocument(null, null);
      fail("addDocument() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dn.getFileContent(null, null);
      fail("getFileContent() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dn.getFileSet(null);
      fail("getFileSet() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dn.createIndex(null);
      fail("addIndex() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dn.commitVersion(null);
      fail("commitVersion() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dn.removeDocuments(null, null);
      fail("removeDocuments() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void testAddDocument() throws Exception {
    dn.addDocument(DNT_INDEX_ONE, new WDocument(makeDocument("name",
        "john smith")));
    // commit the index
    IndexVersion committedIndex = dn.commitVersion(DNT_INDEX_ONE);
    // query the index
    SearchResults hits = dn.search(committedIndex, new WQuery(TestUtils
        .getQuery("name", "smith")), new WSort(new Sort()), 10);
    // check the query returns the original document
    assertEquals(1, hits.size());
    assertEquals("john smith", hits.get(0).get("name"));
  }

  public void testDoHeartbeat() throws Exception {
    dn.doHeartbeat();
  }

  public void testGetFileContent() throws Exception {
    IndexVersion iv = new IndexVersion(DNT_INDEX_ONE);
    String[] files = dn.getFileSet(iv);
    assert (files.length > 0);
    byte[] file = dn.getFileContent(iv, files[0]);
    assert (file != null);
    assert (file.length > 0);
  }

  public void testGetFileSet() throws Exception {
    assertTrue(dn.getFileSet(new IndexVersion(DNT_INDEX_ONE)) != null);
    try {
      dn.getFileSet(new IndexVersion(DNT_INDEX_THREE, 1));
      fail("Should throw an exception");
    } catch (IOException expected) {
      //
    }
  }

  public void testGetProtocolVersion() throws Exception {
    assertEquals(ClientToDataNodeProtocol.VERSION_ID, dn.getProtocolVersion(
        ClientToDataNodeProtocol.class.getName(), 0));
    assertEquals(DataNodeToDataNodeProtocol.VERSION_ID, dn.getProtocolVersion(
        DataNodeToDataNodeProtocol.class.getName(), 0));
  }

  public void testAddIndex() throws Exception {
    dn.createIndex(DNT_INDEX_TWO);
    assertTrue(dn.getFileSet(new IndexVersion(DNT_INDEX_TWO)) != null);
    try {
      dn.createIndex(DNT_INDEX_TWO);
      fail("addIndex() should have thrown an exception");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testCommitVersion() throws Exception {
    // try committing a version with no changes, see what happens
    try {
      dn.commitVersion(DNT_INDEX_ONE);
      fail("commitVersion should have thrown an exception");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testRemoveDocuments() throws Exception {
    dn.addDocument(DNT_INDEX_ONE, new WDocument(makeDocument("name",
        "john smith")));
    // commit the index
    IndexVersion committedIndex = dn.commitVersion(DNT_INDEX_ONE);
    // query the index
    SearchResults hits = dn.search(committedIndex, new WQuery(TestUtils
        .getQuery("name", "smith")), new WSort(new Sort()), 10);
    // check the query returns the original document
    int k = hits.size();
    assertEquals("john smith", hits.get(0).get("name"));
    int j = dn.removeDocuments(DNT_INDEX_ONE, new WTerm(new Term("name", "smith")));
    assertEquals(k, j); 
    committedIndex = dn.commitVersion(DNT_INDEX_ONE);
    hits = dn.search(committedIndex, new WQuery(getQuery("name", "smith")),
        new WSort(new Sort()), 10);
    // check the query returns the original document
    assertEquals(0, hits.size());
  }

  public void testSearch() throws Exception {
    dn.addDocument(DNT_INDEX_ONE, new WDocument(makeDocument("name",
        "john smith")));
    Document doc = new Document();
    Field field = new Field("name", NAME[0], Field.Store.YES,
        Field.Index.TOKENIZED);
    doc.add(field);
    dn.addDocument(DNT_INDEX_ONE, new WDocument(doc));
    IndexVersion committedIndex = dn.commitVersion(DNT_INDEX_ONE);
    SearchResults hits = dn.search(committedIndex, new WQuery(TestUtils
        .getQuery("name", "fred")), new WSort(new Sort()), 10);
    // check the query returns the original document
    assertEquals(1, hits.size());
    assertEquals(NAME[0], hits.get(0).get("name"));
  }

}
