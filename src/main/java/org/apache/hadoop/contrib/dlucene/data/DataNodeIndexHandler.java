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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNode;
import org.apache.hadoop.contrib.dlucene.DataNodeConfiguration;
import org.apache.hadoop.contrib.dlucene.DataNodeToDataNodeProtocol;
import org.apache.hadoop.contrib.dlucene.DataNodeToNameNodeProtocol;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.Lease;
import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.ipc.RPC;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;

/**
 * This class manages Lucene indexes on a {@link DataNode}.
 */
public class DataNodeIndexHandler {

  /** The indexes. */
  private DataNodeIndexes indexes = null;

  /** Log file for this node. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.data.DataNodeLuceneHandler");

  /** The Lucene analyzer to use. */
  private Analyzer analyzer = null;

  /** Hadoop configuration file. */
  private Configuration conf = null;

  /** The DataNode configuration. */
  private DataNodeConfiguration dataconf = null;

  /** Structure holding the ram indexes. */
  private static Map<File, RAMDirectory> ramIndexes = null;

  /** Use the ram index or the filesystem? */
  private boolean useRamIndex ;
    
  /** The lease manager. */
  private DataNodeLeaseManager leases = null;

  /**
   * Constructor.
   * 
   * @param dataNodeConfiguration the datanode configuration
   * @param configuration the Hadoop configuration
   * @param analyzer Lucene analzyer
   * @param useRamIndex use RAM based Lucene index?
   * @throws IOException
   */
  public DataNodeIndexHandler(DataNodeConfiguration dataNodeConfiguration,
      Configuration configuration, Analyzer analyzer, boolean useRamIndex, DataNodeToNameNodeProtocol namenode)
      throws IOException {
    this.conf = configuration;
    this.analyzer = analyzer;
    this.dataconf = dataNodeConfiguration;
    if (ramIndexes == null && useRamIndex) {
      ramIndexes = new HashMap<File, RAMDirectory>();
    }
    this.useRamIndex = useRamIndex;
    indexes = new DataNodeIndexes(dataNodeConfiguration);
    leases = new DataNodeLeaseManager(namenode, dataconf.getAddress());

    // the directory structure is we have root/id/version
    String[] files = dataNodeConfiguration.getRootDir().list(
        new FilenameFilter() {
          public boolean accept(final File dir, final String name) {
            return !name.startsWith(".");
          }
        });
    if (files != null && !useRamIndex) {
      for (String indexName : files) {
        File index = new File(dataNodeConfiguration.getRootDir(), indexName);
        Utils.checkDirectoryIsReadableWritable(index);
        for (String version : index.list()) {
          File versionDirectory = new File(index, version);
          if (indexExists(versionDirectory)) {
            Utils.checkDirectoryIsReadableWritable(versionDirectory);
            if (version.startsWith(Constants.VERSION_PREFIX)) {
              int versionNumber = Integer.parseInt(version
                  .substring(Constants.VERSION_PREFIX.length()));

              IndexLocation location = new IndexLocation(dataNodeConfiguration
                  .getAddress(), new IndexVersion(indexName, versionNumber),
                  IndexState.LIVE);
              indexes.add(location);
            } else {
              throw new IOException("Index directory " + version
                  + " does not begin with version number");
            }
          }
        }
      }
    }
  }
  
  /**
   * @return get all the leases currently used by this datanode
   */
  public Lease[] getLeases() {
    return leases.getLeases();
  }
  
  /**
   * Get all the files used in a specific index.
   * 
   * @param indexVersion the index
   * @return all the files used in the index
   * @throws IOException
   */
  public String[] getFileSet(IndexVersion indexVersion) throws IOException {
    IndexReader reader = null;
    Directory directory = null;
    String[] results = null;
    try {
      File dir = indexes.getKnownIndexDirectory(indexVersion);
      reader = getIndexReader(dir);
      directory = reader.directory();
      results = directory.list();
    } finally {
      close(reader, directory, null, null);
    }
    return results;
  }

  /**
   * Get a particular file from a specific index.
   * 
   * @param indexVersion the index
   * @param file the file
   * @return the data in the file
   * @throws IOException
   */
  public byte[] getFileContent(IndexVersion indexVersion, String file)
      throws IOException {
    IndexReader reader = null;
    Directory directory = null;
    IndexInput is = null;
    // FIXME Is there a problem here, because the max size of buf is an int ?
    byte[] buf = null;

    try {
      File dir = indexes.getKnownIndexDirectory(indexVersion);
      reader = getIndexReader(dir);
      directory = reader.directory();
      try {
        // read current file
        is = directory.openInput(file);
        int len = (int) is.length(); // FIXME - ugly cast here
        buf = new byte[len];
        is.readBytes(buf, 0, len);
      } finally {
        if (is != null)
          is.close();
      }
    } finally {
      close(reader, directory, null, null);
    }
    return buf;
  }

  /**
   * Add a document to an index.
   * 
   * @param indexName The index
   * @param document The document
   * @throws IOException
   */
  public void addDocument(String indexName, Document document)
      throws IOException {
    if (indexes.hasIndex(indexName)) {
      IndexWriter writer = null;
      try {
        writer = openNewIndexVersion(indexName);
        LOG.debug("Before adding document " + indexName + " contains "
            + writer.docCount() + " documents.");
        writer.addDocument(document);
        LOG.debug("After adding document " + indexName
            + " contains " + writer.docCount() + " documents.");
      } finally {
        close(null, null, writer, null);
      }
    } else {
      throw new IOException("Datanode does not have index " + indexName);
    }
  }

  /**
   * Create an index on the datanode.
   * 
   * @param indexName the name of the index
   * @return the new index
   * @throws IOException
   */
  public IndexVersion createIndex(String indexName) throws IOException {
    IndexWriter targetWriter = null;
    IndexVersion sourceIndexVersion = null;
    if (indexes.hasIndex(indexName)) {
      throw new IOException("Index " + indexName + " already exists");
    }
    try {
      sourceIndexVersion = new IndexVersion(indexName);
      IndexLocation location = new IndexLocation(dataconf.getAddress(),
          sourceIndexVersion, IndexState.LIVE);
      indexes.add(location);
      File newIndex = indexes.getIndexDirectory(sourceIndexVersion);
      targetWriter = getIndexWriter(newIndex, true);
      LOG.info("Created index " + location);
    } finally {
      close(null, null, targetWriter, null);
    }
    return sourceIndexVersion;
  }
  
  /**
   * Delete all the indexes associated with this name
   * 
   * @param indexName
   * @return
   * @throws IOException
   */
  public boolean deleteIndex(String indexName) throws IOException {
    return indexes.deleteIndexes(indexName);
  }

  /**
   * Add an existing remote index to an index on this datanode.
   *
   * @param indexName destination
   * @param source source remote index
   * @throws IOException
   */
  public void addIndex(String indexName, IndexLocation source)
      throws IOException {
    if (indexes.hasIndex(indexName)) {
      // first replicate the index locally
      copyRemoteIndex(source);
      IndexReader reader = null;
      IndexWriter writer = null;
      try {
        writer = openNewIndexVersion(indexName);
        File dir = indexes.getKnownIndexDirectory(source.getIndexVersion());
        reader = getIndexReader(dir);
        IndexReader[] readers = new IndexReader[1];
        readers[0] = reader;
        writer.addIndexes(readers);
      } finally {
        close(reader, null, writer, null);
      }
    } else {
      throw new IOException("DataNode does not have index " + indexName);
    }
  }

  /**
   * Perform a query on an index.
   * 
   * @param indexVersion the index
   * @param query the query
   * @param sort how to sort the results
   * @param n the number of results
   * @return the query results
   * @throws IOException
   */
  public SearchResults search(IndexVersion indexVersion, Query query,
      Sort sort, int n) throws IOException {
    IndexReader reader = null;
    Directory directory = null;
    SearchResults hits = null;
    IndexSearcher searcher = null;

    try {
      File dir = indexes.getKnownIndexDirectory(indexVersion);
      reader = getIndexReader(dir);
      directory = reader.directory();
      searcher = new IndexSearcher(directory);
      // FIXME - for now we throw away n, not sure how to use it
      // and still return hits with Lucene API
      Hits h1 = searcher.search(query, sort);
      LOG.info("Found " + h1.length() + " search results");
      hits = new SearchResults(h1);
    } finally {
      close(reader, directory, null, searcher);
    }
    return hits;
  }
  
  public int size(String index) throws IOException {
    return size(indexes.getPrimaryIndex(index).getIndexVersion());
  }
    

  public int size(IndexVersion indexVersion) throws IOException {
    IndexReader reader = null;
    Directory directory = null;
    int num = 0;

    try {
      File dir = indexes.getKnownIndexDirectory(indexVersion);
      reader = getIndexReader(dir);
      num = reader.numDocs();
    } finally {
      close(reader, directory, null, null);
    }
    return num;
  }

  // FIXME I changed this from returning int[] to return int
  // because that was closer to the Lucene API
  /**
   * Remove documents from this index.
   * 
   * @param indexName the index
   * @param term A Lucene Term used to identify the documents to remove
   * @return the number of documents remove
   * @throws IOException
   */
  public int removeDocuments(String indexName, Term term) throws IOException {
    //      FIXME - shouldn't this make a new version of an index?
    
    // not sure what the return value is, lucene returns int on a reader
    // but void on a writer
    IndexVersion iv = indexes.getPrimaryIndex(indexName).getIndexVersion();
    leases.getLease(iv);
    int results;
    IndexReader reader = null;
    Directory directory = null;
    Hits hits = null;
    IndexSearcher searcher = null;
    try {
      File dir = indexes.getKnownIndexDirectory(iv);
      reader = getIndexReader(dir);
      directory = reader.directory();
      searcher = new IndexSearcher(directory);
      hits = searcher.search(new TermQuery(term));
      results = hits.length();
      LOG.info("removeDocuments: found " + results + " matching documents");
    } finally {
      close(reader, directory, null, searcher);
    }
    // only create a new version of index if some documents will be deleted
    if (results > 0) {
      IndexWriter writer = openNewIndexVersion(indexName);
      Directory file = writer.getDirectory();
      close(null, null, writer, null);
      try {
        reader = IndexReader.open(file);
        results = reader.deleteDocuments(term);
      } finally {
        close(reader, null, null, null);
      }
    }
    return results;
  }

  /**
   * If needed, open a new version of an index for committing changes.
   * 
   * @param indexName the index
   * @return a Lucene IndexWriter
   * @throws IOException
   */
  IndexWriter openNewIndexVersion(String indexName) throws IOException {
    IndexWriter writer = null;
    IndexVersion iv = indexes.getPrimaryIndex(indexName).getIndexVersion();
    IndexLocation index = indexes.getPrimaryIndex(indexName);
    if (!index.getState().equals(IndexState.UNCOMMITTED)) {
      LOG.debug("Creating a new index");
      // there is no version of this index already open with uncommitted changes
      IndexVersion newVersion = iv.nextVersion();
      IndexLocation location = new IndexLocation(dataconf.getAddress(),
          newVersion, IndexState.UNCOMMITTED);
      if (leases.getLease(newVersion)) {
        File directory = indexes.getIndexDirectory(newVersion);
        LOG.debug("Destination directory is " + newVersion);
        writer = getIndexWriter(directory, true);
        Directory targetDirectory = null;
        try {
          // there are no uncommitted changes - make a new version
          targetDirectory = writer.getDirectory();
          copyIndex(iv, targetDirectory);
          indexes.add(location);
          LOG.debug("Copied from " + iv + " to " + targetDirectory);
          iv = newVersion;
        } finally {
          close(null, targetDirectory, writer, null);
        }
      }
    } else {
      LOG.debug("Index already exists");
    }

    if (leases.getLease(iv)) {
      return getIndexWriter(indexes.getIndexDirectory(iv),
        false);
    } 
    throw new IOException("Could not open index " + indexName);
  }
  
  IndexVersion getNewIndexVersion(String indexName) {
    IndexVersion iv = indexes.getPrimaryIndex(indexName).getIndexVersion();
    IndexLocation index = indexes.getPrimaryIndex(indexName);
    if (!index.getState().equals(IndexState.UNCOMMITTED)) {
      LOG.debug("Creating a new index");
      // there is no version of this index already open with uncommitted changes
      iv = iv.nextVersion();
    } else {
      LOG.debug("Index already exists");
    }
    return iv;
  }
  
  /**
   * Get the directory of an index.
   * 
   * @param index the index.
   * @return the directory
   */
  public File getIndexDirectory(IndexVersion index) {
    return indexes.getIndexDirectory(index);
  }

  /**
   * Copy an index to a target directory.
   * 
   * @param indexVersion the index
   * @param targetDirectory the target directory
   * @param newIndex 
   * @return the files to be copied
   * @throws IOException
   */
  Set<String> copyIndex(IndexVersion indexVersion, Directory targetDirectory,
      File newIndex) throws IOException {
    Set<String> localFiles = new HashSet<String>();
    IndexReader reader = null;
    copyIndex(indexVersion, targetDirectory);
    try {
      reader = getIndexReader(newIndex);
      Directory directory = reader.directory();
      for (String localFile : directory.list()) {
        localFiles.add(localFile);
      }
    } finally {
      close(reader, null, null, null);
    }
    return localFiles;
  }

  /**
   * Copy an index from a location to a target directory.
   * 
   * @param indexVersion the index
   * @param targetDirectory the target directory
   * @throws IOException
   */
  private void copyIndex(IndexVersion indexVersion, Directory targetDirectory)
      throws IOException {
    File source = indexes.getIndexDirectory(indexVersion);
    LOG.info("Source is " + source.getAbsolutePath());
    Directory sourceDirectory = null;
    IndexReader sourceReader = null;
    try {
      sourceReader = getIndexReader(source);
      LOG.debug("Copying index - there are " + sourceReader.numDocs()
          + " documents in source index");
      sourceDirectory = sourceReader.directory();
      Directory.copy(sourceDirectory, targetDirectory, false);
      LOG.debug("Copied from " + indexVersion + " to " + targetDirectory);
      String[] files = targetDirectory.list();
      for (String s : files) {
        LOG.debug("Copied " + s);
      }
    } finally {
      close(sourceReader, sourceDirectory, null, null);
    }
  }

  /**
   * Copy a remote index to this machine. If a older local copy exists, take
   * advantage of it to minimize network traffic.
   * 
   * @param indexToCopy the index to copy
   * @throws IOException
   */
  public void copyRemoteIndex(IndexLocation indexToCopy) throws IOException {
    if (indexes.hasIndex(indexToCopy.getIndexVersion())) {
      // node already has index!
      LOG.info("Data node is already replicating " + indexToCopy.toString()
          + "\n");
    } else {
      DataNodeToDataNodeProtocol datanode = (DataNodeToDataNodeProtocol) RPC
          .waitForProxy(DataNodeToDataNodeProtocol.class,
              DataNodeToDataNodeProtocol.VERSION_ID, indexToCopy.getAddress(),
              conf);
      Directory targetDirectory = null;
      IndexWriter targetWriter = null;
      IndexVersion indexVersionToCopy = indexToCopy.getIndexVersion();
      String sourceId = indexVersionToCopy.getName();
      IndexVersion primaryVersion = null;
      if (indexes.hasIndex(sourceId)) {
        primaryVersion = indexes.getPrimaryIndex(sourceId).getIndexVersion();
      }

      try {
        IndexLocation location = new IndexLocation(dataconf.getAddress(),
            indexVersionToCopy, IndexState.REPLICATING);
        indexes.add(location);
        File newIndex = indexes.getIndexDirectory(indexVersionToCopy);
        targetWriter = getIndexWriter(newIndex, true);
        targetDirectory = targetWriter.getDirectory();
        String[] remoteFiles = datanode.getFileSet(indexVersionToCopy);
        // if there is a local version, copy that first as it is quicker
        // then get the remote diffs
        Set<String> localFiles = new HashSet<String>();
        if (primaryVersion != null) {
          localFiles = copyIndex(primaryVersion, targetDirectory, newIndex);
        }
        // copy all the files that are not in the local index
        for (String remoteFile : remoteFiles) {
          if (!localFiles.contains(remoteFile)
              || remoteFile.startsWith("segments")
              || remoteFile.startsWith("_")) {
            byte[] file = datanode.getFileContent(indexVersionToCopy,
                remoteFile);
            copyRemoteFile(remoteFile, file, targetDirectory);
          }
        }
        indexes
            .setIndexState(location, IndexState.REPLICATING, IndexState.LIVE);
      } finally {
        close(null, targetDirectory, targetWriter, null);
      }
    }
  }

  /**
   * Copy a remote file to a target directory.
   * 
   * @param remoteFile the name of the remote file
   * @param file the contents of the file
   * @param targetDirectory the target directory
   * @throws IOException
   */
  private static void copyRemoteFile(String remoteFile, byte[] file,
      Directory targetDirectory) throws IOException {
    IndexOutput os = null;
    LOG.info("Copying " + remoteFile + " to " + targetDirectory);
    try {
      // create file in dest directory
      os = targetDirectory.createOutput(remoteFile);
      // and copy to dest directory
      long len = file.length;
      long readCount = 0;
      while (readCount < len) {
        int toRead = readCount + Constants.BUFFER_SIZE > len ? (int) (len - readCount)
            : Constants.BUFFER_SIZE;
        os.writeBytes(file, toRead);
        readCount += toRead;
      }
    } finally {
      if (os != null)
        os.close();
    }
  }

  /**
   * Commit an index.
   * 
   * @param index the index to be committed
   * @return the committed index
   * @throws IOException
   */
  public IndexVersion commitVersion(String index) throws IOException {
    IndexVersion iv = indexes.getPrimaryIndex(index).getIndexVersion();
    IndexLocation location = indexes.getPrimaryIndex(index);
    indexes.setIndexState(location, IndexState.UNCOMMITTED, IndexState.LIVE);
    leases.relinquishLease(iv);
    return iv;
  }

  /**
   * Get all the indexes stored on this datanode.
   * 
   * @return
   */
  public IndexLocation[] getIndexes() {
    return indexes.getIndexes();
  }

  /**
   * Does this index exist?
   * 
   * @param directory the directory storing the index
   * @return does the index exist?
   */
  private boolean indexExists(File directory) {
    return !useRamIndex ? IndexReader.indexExists(directory) : ramIndexes
        .containsKey(directory);
  }

  /**
   * Get the IndexWriter for this index.
   * 
   * @param directory the directory storing the index
   * @param create create the index or use the existing index
   * @return the IndexWriter
   * @throws IOException
   */
  IndexWriter getIndexWriter(File directory, boolean create) throws IOException {
    IndexWriter writer = null;
    if (!useRamIndex) {
      writer = new IndexWriter(directory, analyzer, create);
    } else {
      if (create) {
      ramIndexes.put(directory, new RAMDirectory());
      }
      RAMDirectory rd = ramIndexes.get(directory);
      LOG.debug("Directory is " + directory + " RamDirectory is " + rd);
      writer = new IndexWriter(rd, analyzer, create);
    }
    LOG.debug("IndexWriter is " + writer.toString());
    return writer;
  }

  /**
   * Get the IndexReader for this index.
   * 
   * @param directory the directory storing the index
   * @return the IndexReader
   * @throws IOException
   */
  IndexReader getIndexReader(File directory) throws IOException {
    return !useRamIndex ? IndexReader.open(directory) : IndexReader
        .open(ramIndexes.get(directory));
  }

  /**
   * Close index readers, directories, writers or searchers.
   * 
   * @param reader the Lucene IndexReader
   * @param directory the Lucene Directory
   * @param writer the Lucene IndexWriter
   * @param searcher the Lucene IndexSearcher
   * @throws IOException
   */
  private void close(IndexReader reader, Directory directory,
      IndexWriter writer, IndexSearcher searcher) throws IOException {
    if (directory != null && !useRamIndex)
      directory.close();
    if (reader != null)
      reader.close();
    if (writer != null)
      writer.close();
    if (searcher != null)
      searcher.close();
  }
}
