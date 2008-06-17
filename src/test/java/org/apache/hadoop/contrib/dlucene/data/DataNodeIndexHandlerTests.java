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

package org.apache.hadoop.contrib.dlucene.data;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNode;
import org.apache.hadoop.contrib.dlucene.DataNodeConfiguration;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.MiniDLuceneCluster;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.contrib.dlucene.writable.WDocument;
import org.apache.hadoop.contrib.dlucene.writable.WQuery;
import org.apache.hadoop.contrib.dlucene.writable.WSort;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;

public class DataNodeIndexHandlerTests extends TestUtils {

  private static DataNodeIndexHandler dnlh = null;
  private static MiniDLuceneCluster cluster = null;
  private DataNodeConfiguration dnc = null;

  protected void setUp() throws Exception {
    super.setUp();
    int port = conf.getInt(Constants.DATANODE_PORT_NAME,
        Constants.DLUCENE_DATANODE_PORT);
    InetSocketAddress addr = new InetSocketAddress(Constants.HOST, port);
    dnc = new DataNodeConfiguration(addr, NetworkTopology.DEFAULT_RACK,Constants.DEFAULT_ROOT_DIR);
    if (dnlh == null) {
      cluster = new MiniDLuceneCluster(conf, 2);
      final ZKClient zkclient = new ZKClient(new ZkConfiguration());
      dnlh = new DataNodeIndexHandler(zkclient, dnc, conf, new StandardAnalyzer(),
          USE_RAM_INDEX_FOR_TESTS);
    }
  }
  
  private String setupOne() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[2]));
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[3]));
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[5]));
    return index1;
  }

  public void testAddIndexStringIndexLocation() throws Exception {
    String index1 = setupOne();
    String index3 = getNextIndex();
    DataNode m1 = cluster.getDataNode(0);
    m1.createIndex(index3);
    m1.addDocument(index3, new WDocument(makeDocument(FIELD, NAME[6])));
    m1.addDocument(index3, new WDocument(makeDocument(FIELD, NAME[8])));
    IndexVersion i = m1.commitVersion(index3);
    IndexLocation location = getLocation(0, i);
    dnlh.addIndex(index1, location);
    IndexVersion iv = dnlh.commitVersion(index1);
    SearchResults hits = dnlh.search(iv, getQuery(FIELD, FORENAME[6]),
        new Sort(), 10);
    assertEquals(1, hits.size());

    try {
      dnlh.addIndex(UNKNOWN_INDEX, location);
      fail("Should throw an exception here");
    } catch (Exception e) {
      //
    }
  }

  public void testCreateIndex() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[4]));
    IndexVersion committedIndex = dnlh.commitVersion(index1);
    assertEquals(1, committedIndex.getVersion());
    SearchResults hits = dnlh.search(committedIndex,
        getQuery(FIELD, SURNAME[4]), new Sort(), 10);
    assertEquals(1, hits.size());
    assertEquals(NAME[4], hits.get(0).get(FIELD));
    try {
      dnlh.createIndex(index1);
      fail("Should throw an exception");
    } catch (Exception e) {
      //
    }
  }

  public void testGetFileSet() throws Exception {
    IndexLocation[] indexes = dnlh.getIndexes();
    assertTrue(indexes.length > 0);
    for (IndexLocation index : indexes) {
      String[] files = dnlh.getFileSet(index.getIndexVersion());
      assertTrue(files.length > 0);
    }
  }

  public void testGetFileContent() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    IndexVersion iv = new IndexVersion(index1);
    String[] files = dnlh.getFileSet(iv);
    assert (files.length > 0);
    byte[] file = dnlh.getFileContent(iv, files[0]);
    assert (file != null);
    assert (file.length > 0);
  }

  public void testAddDocumentUnknown() throws Exception {
    try {
      dnlh.addDocument(UNKNOWN_INDEX, makeDocument(FIELD, NAME[4]));
      fail("Should throw an exception here");
    } catch (Exception e) {
      //
    }
  }

  public void testAddDocument() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[4]));
    IndexVersion committedIndex = dnlh.commitVersion(index1);
    SearchResults hits = dnlh.search(committedIndex,
        getQuery(FIELD, SURNAME[4]), new Sort(), 10);
    assertEquals(1, hits.size());
    assertEquals(NAME[4], hits.get(0).get(FIELD));
  }

  public void testCommitIndexNull() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    // try committing a version with no changes, see what happens
    try {
      dnlh.commitVersion(index1);
      fail("commitVersion should have thrown an exception");
    } catch (Exception expected) {
      // expected
    }
  }

  public void testGetArrayOfIndexes() throws Exception {
    String[] indexes = { getNextIndex(), getNextIndex(), getNextIndex() };
    for (int i = 0; i < indexes.length; i++) {
      dnlh.createIndex(indexes[i]);
    }
    Set<IndexLocation> il = toSet(dnlh.getIndexes());
    Set<IndexVersion> ij = new HashSet<IndexVersion>();
    for (IndexLocation j : il) {
      ij.add(j.getIndexVersion());
    }
    for (int k = 0; k < indexes.length; k++) {
      assertTrue(ij.contains(new IndexVersion(indexes[k], 0)));
    }
  }

  public void testSearch() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[4]));
    dnlh.addDocument(index1, makeDocument(FIELD, NAME[0]));
    IndexVersion committedIndex = dnlh.commitVersion(index1);
    SearchResults hits = dnlh.search(committedIndex, getQuery(FIELD,
        FORENAME[0]), new Sort(), 10);
    // check the query returns the original document
    assertEquals(1, hits.size());
    assertEquals(NAME[0], hits.get(0).get(FIELD));
  }

  public void testRemoveDocuments() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    for (int i = 1 ; i < 6; i++) {
      dnlh.addDocument(index1, makeDocument(FIELD, NAME[i]));
    }
    IndexVersion committedIndex = dnlh.commitVersion(index1);
    assertEquals(5, dnlh.size(committedIndex));
    SearchResults hits = dnlh.search(committedIndex,
        getQuery(FIELD, SURNAME[4]), new Sort(), 10);
    assertEquals(1, hits.size());
    assertEquals(NAME[4], hits.get(0).get(FIELD));
    System.out.println("Prior to delete, index contains " + dnlh.size(committedIndex) + " documents");
    dnlh.removeDocuments(index1, new Term(FIELD, SURNAME[4]));
    committedIndex = dnlh.commitVersion(index1);
    System.out.println("After delete, index contains " + dnlh.size(committedIndex) + " documents");
    hits = dnlh.search(committedIndex, getQuery(FIELD, SURNAME[4]), new Sort(),
        10);
    // check the query returns the original document
    assertEquals(0, hits.size());
    assertEquals(4, dnlh.size(committedIndex));
  }

  public void testOpenNewIndexVersion() throws Exception {
    String index1 = getNextIndex();
    dnlh.createIndex(index1);
    IndexWriter iw = dnlh.openNewIndexVersion(index1);
    assert (iw != null);
    iw.close();
    IndexLocation[] indexes = dnlh.getIndexes();
    for (IndexLocation il : indexes) {
      System.out.println(il);
    }
  }

  public void testCopyRemoteIndex() throws Exception {
    String newIndex = getNextIndex();
    DataNode m1 = cluster.getDataNode(1);
    m1.createIndex(newIndex);
    m1.addDocument(newIndex, new WDocument(makeDocument(FIELD, NAME[7])));
    IndexVersion i = m1.commitVersion(newIndex);
    SearchResults hits = m1.search(i, new WQuery(getQuery(FIELD, FORENAME[4])),
        new WSort(new Sort()), 10);
    assertEquals(1, hits.size());

    IndexLocation location = getLocation(1, i);
    dnlh.copyRemoteIndex(location);
    Set<IndexLocation> il = toSet(dnlh.getIndexes());
    Set<IndexVersion> ij = new HashSet<IndexVersion>();
    for (IndexLocation j : il) {
      ij.add(j.getIndexVersion());
    }
    hits = dnlh.search(i, getQuery(FIELD, FORENAME[4]), new Sort(), 10);
    assertEquals(1, hits.size());
  }

  public void testCopyIndex() throws Exception {
    String index1 = getNextIndex();
    IndexVersion iv1 = dnlh.createIndex(index1);
    IndexVersion ivT = iv1.nextVersion();
    for (int i = 1; i < 4; i++) {
      dnlh.addDocument(index1, makeDocument(FIELD, NAME[i]));
      assertEquals(i, dnlh.size(ivT));
    }
    IndexVersion iv2 = dnlh.commitVersion(index1);
    assertEquals(3, dnlh.size(iv2));
    IndexLocation location = getLocation(0, iv2);
    
    Directory targetDirectory = null;
    String tc = "testCopyIndex";
    IndexVersion iv3 = dnlh.createIndex(tc);    
    File newIndex = dnlh.getIndexDirectory(iv3);
    IndexWriter targetWriter = dnlh.getIndexWriter(newIndex, true);
    targetDirectory = targetWriter.getDirectory();
    Set<String> results = dnlh.copyIndex(location.getIndexVersion(), targetDirectory, newIndex);
    String[] files = dnlh.getFileSet(iv2);
    for (String file : files) {
      assertTrue(results.contains(file));
    }
    targetWriter.close();
    IndexReader reader = dnlh.getIndexReader(newIndex);
    assertEquals(3, reader.numDocs());
    IndexVersion iv4 = iv3.nextVersion();
    
    for (int i = 4; i < 6; i++) {
      dnlh.addDocument(tc, makeDocument(FIELD, NAME[i]));
      System.out.println(iv3 + " size " + dnlh.size(iv3));
      System.out.println(iv4 + " size " + dnlh.size(iv4));
      assertEquals(i, dnlh.size(iv4));
    }
    IndexVersion iv5 = dnlh.commitVersion(tc);
    System.out.println(iv5);
    assertEquals(5, dnlh.size(iv5));
  }

  IndexLocation getLocation(int datanode, IndexVersion iv) {
    InetSocketAddress addr = NetUtils.createSocketAddr(Constants.HOST + ":"
        + cluster.getDataNodePort(datanode));
    IndexLocation location = new IndexLocation(addr, iv, IndexState.LIVE);
    return location;
  }

  public void testNewDataNode() throws Exception {
    final ZKClient zkclient = new ZKClient(new ZkConfiguration());
    new DataNodeIndexHandler(zkclient, dnc, conf, new StandardAnalyzer(),
        USE_RAM_INDEX_FOR_TESTS);
  }
  
}
