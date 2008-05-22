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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.data.Indexes;
import org.apache.hadoop.contrib.dlucene.data.ShardedIndexes;
import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.contrib.dlucene.writable.WDocument;
import org.apache.hadoop.contrib.dlucene.writable.WQuery;
import org.apache.hadoop.contrib.dlucene.writable.WSort;
import org.apache.hadoop.contrib.dlucene.writable.WTerm;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

/**
 * The client library caches all the searchable indexes.
 * 
 * It supports sharded indexes. Sharded indexes are distinguished from unsharded
 * indexes because they contain a hyphen and a number
 * 
 * Added documents are sent to a shard selected at random. Once a new version of
 * an index has been created with uncommitted changes, all the changes from this
 * client are sent to them.
 * 
 * Searches are sent to each shard and the results are merged.
 * 
 * Deletions are sent to each shard.
 */
public class CachedClient extends SimpleClient implements ICachedClient {

  /** Logging. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.CachedClient");

  /** When was the cache last updated? */
  private long cacheLastUpdated = System.currentTimeMillis();

  /** How long to cache index information. */
  private long cacheLease = 5000;

  /** A cache of the index locations. */
  private Set<IndexVersion> versions = null;

  /** Index names hiding sharding. */
  private Set<String> indexNames = null;

  /** The data structure managing the indexes. */
  private Indexes indexes = null;

  /** Sharded indexes. */
  private ShardedIndexes shardedIndexes = null;

  /** Interface to access namenode. */
  protected ClientToNameNodeProtocol namenode = null;

  /**
   * Constructor.
   * 
   * @param conf the Hadoop configuration
   * @throws IOException thrown if cannot contact the NameNode
   */
  public CachedClient(Configuration conf) throws IOException {
    this(conf, NetUtils
        .createSocketAddr(conf.get(Constants.NAMENODE_DEFAULT_NAME,
            Constants.NAMENODE_DEFAULT_NAME_VALUE)));
  }

  /**
   * Constructor.
   * 
   * @param configuration the Hadoop configuration
   * @param addr the address of the NameNode
   * @throws IOException thrown if cannot contact the NameNode
   */
  public CachedClient(Configuration configuration, InetSocketAddress addr)
      throws IOException {
    super(configuration);
    this.namenode = getNameNode(addr);
    updateCache(true);
  }

  public String toString() {
    return indexes.toString();
  }

  // FIXME - how do we ensure shards are on different machines?
  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.ICachedClient#createIndex(java.lang.String, boolean)
   */
  public void createIndex(String index, boolean sharded) throws IOException {
    Utils.checkArgs(index);
    updateCache(false);
    if (!indexNames.contains(index) || sharded) {
      String s = namenode.getDataNode();
      InetSocketAddress address = NetUtils.createSocketAddr(s);
      String name = sharded ? shardedIndexes.createShardedIndex(index) : index;
      ClientToDataNodeProtocol p = getDataNode(address);
      LOG.info("Creating index " + index + " on " + s + " as " + name);
      IndexVersion iv = p.createIndex(name);
      addIndex(new IndexLocation(address, iv, IndexState.LIVE));
    } else {
      throw new IOException("Index already exists");
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.ICachedClient#getIndexUpdater(java.lang.String)
   */
  public IIndexUpdater getIndexUpdater(String index) {
    return new IndexUpdater(index);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.ICachedClient#size(java.lang.String)
   */
  public int size(String index) throws IOException {
    Utils.checkArgs(index);
    updateCache(false);
    int results = 0;
    if (shardedIndexes.isSharded(index)) {
      for (String shard : shardedIndexes.getShards(index)) {
        results += getDataNode(indexes.getIndexLocation(shard).getAddress())
            .size(shard);
      }
    } else {
      results += getDataNode(indexes.getIndexLocation(index).getAddress())
          .size(index);
    }
    return results;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.ICachedClient#search(java.lang.String, org.apache.lucene.search.Query, org.apache.lucene.search.Sort, int)
   */
  public SearchResults search(String index, Query query, Sort sort, int n)
      throws IOException {
    Utils.checkArgs(index, query, sort);
    updateCache(false);
    SearchResults results = new SearchResults();
    WQuery wquery = new WQuery(query);
    WSort wsort = new WSort(sort);
    if (shardedIndexes.isSharded(index)) {
      String[] shards = shardedIndexes.getShards(index);
      Vector<InetSocketAddress> addresses = new Vector<InetSocketAddress>();
      Object[][] params = new Object[shards.length][4];
      for (int i = 0; i < shards.length; i++) {
        IndexLocation location = indexes.getIndexLocation(shards[i]);
        addresses.add(location.getAddress());
        params[i][0] = location.getIndexVersion();
        params[i][1] = wquery;
        params[i][2] = wsort;
        params[i][3] = n;
      }
      Method search;
      SearchResults[] sr = null;
      try {
        search = ClientToDataNodeProtocol.class.getMethod("search",
            new Class[] { IndexVersion.class, WQuery.class, WSort.class,
                int.class });
        sr = (SearchResults[]) RPC.call(search, params, addresses
            .toArray(new InetSocketAddress[addresses.size()]), conf);
      } catch (SecurityException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      }
      // FIXME the results need merge sorting here
      for (SearchResults sri : sr) {
        results.add(sri);
      }

    } else {
      IndexLocation location = indexes.getIndexLocation(index);
      results.add(getDataNode(location.getAddress()).search(
          location.getIndexVersion(), wquery, wsort, n));
    }
    return results;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.ICachedClient#getIndexes()
   */
  public String[] getIndexes() {
    updateCache(false);
    return indexNames.toArray(new String[indexNames.size()]);
  }

  /**
   * Update the cache.
   * 
   * @param force force cache update
   */
  protected void updateCache(boolean force) {
    long now = System.currentTimeMillis();
    if (force || now > cacheLastUpdated + cacheLease) {
      cacheLastUpdated = now;
      // reset cache
      if (indexes == null) {
        indexes = new Indexes();
        shardedIndexes = new ShardedIndexes();
        indexNames = new HashSet<String>();
        versions = new HashSet<IndexVersion>();
        LOG.info("Updating CachedClient cache");
        for (IndexLocation location : namenode.getSearchableIndexes()) {
          LOG.info(location.toString());
          indexes.add(location, Network.DEFAULT_RACK);
        }
        LOG.info("Finished updating cache");
      }
    }
  }

  /**
   * Add an index to the client cache.
   * 
   * @param location the IndexVersion
   */
  private void addIndex(IndexLocation location) {
    String name = location.getIndexVersion().getName();
    if (name.indexOf(Constants.SHARD_CHAR) >= 0) {
      indexNames.add(name.substring(0, name.indexOf(Constants.SHARD_CHAR)));
    } else {
      indexNames.add(name);
    }
    versions.add(location.getIndexVersion());
    indexes.add(location, Network.DEFAULT_RACK);
  }

  public class IndexUpdater implements IIndexUpdater {
    private String index = null;
    private String currentShard = null;
    private HashMap<String, IndexLocation> shardLocation = new HashMap<String, IndexLocation>();

    /**
     * Constructor
     * 
     * @param index the index to be updated.
     */
    private IndexUpdater(String index) {
      this.index = index;
      currentShard = shardedIndexes.isSharded(index) ? shardedIndexes
          .getRandomShard(index) : index;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.dlucene.IIndexUpdater#addDocument(org.apache.lucene.document.Document)
     */
    public void addDocument(Document doc) throws IOException {
      Utils.checkArgs(doc);
      updateCache(false);
      WDocument wdoc = new WDocument(doc);
      IndexLocation location = getLocation(currentShard);
      InetSocketAddress addr = location.getAddress();
      getDataNode(addr).addDocument(location.getIndexVersion().getName(), wdoc);
      shardLocation.put(currentShard, getLocation(currentShard));
    }

    /**
     * Get an IndexLocation corresponding to an updateable index.
     * 
     * @param index the index name
     * @return the index location of an updateable index
     */
    private IndexLocation getLocation(String theIndex) {
      if (shardLocation.containsKey(theIndex)) {
        return shardLocation.get(theIndex);
      }
      IndexLocation location = indexes.getUncommittedIndex(currentShard);
      if (location != null) {
        LOG.debug("Found uncommitted index for " + theIndex + "  " + location);
      } else {
        IndexVersion oldVersion = indexes.getPrimaryIndex(theIndex);
        IndexLocation oldLocation = indexes.getIndexLocation(oldVersion);
        LOG.debug("Creating new uncommitted index for " + theIndex + "  "
            + oldLocation);
        location = new IndexLocation(oldLocation.getAddress(), oldLocation
            .getIndexVersion().nextVersion(), IndexState.UNCOMMITTED);
        indexes.add(location, Network.DEFAULT_RACK);
      }
      return location;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.dlucene.IIndexUpdater#removeDocuments(org.apache.lucene.index.Term)
     */
    public int removeDocuments(Term term) throws IOException {
      Utils.checkArgs(index, term);
      updateCache(false);
      int result = 0;
      if (shardedIndexes.isSharded(index)) {
        for (String shard : shardedIndexes.getShards(index)) {
          IndexLocation location = getLocation(shard);
          int removedDocuments = getDataNode(location.getAddress())
              .removeDocuments(index, new WTerm(term));
          if (removedDocuments > 0) {
            shardLocation.put(shard, location);
          }
          result += removedDocuments;
        }
        String[] shards = shardedIndexes.getShards(index);
        Vector<InetSocketAddress> addresses = new Vector<InetSocketAddress>();
        Object[][] params = new Object[shards.length][2];
        Integer[] results = new Integer[shards.length];
        IndexLocation[] locations = new IndexLocation[shards.length];
        for (int i = 0; i < shards.length; i++) {
          IndexVersion version = indexes.getPrimaryIndex(shards[i]);
          params[i][0] = version;
          params[i][1] = new WTerm(term);
          locations[i] = indexes.getIndexLocation(version);
          addresses.add(locations[i].getAddress());
        }
        Method removeDocuments;
        try {
          removeDocuments = ClientToDataNodeProtocol.class.getMethod(
              "removeDocuments", new Class[] { String.class, WTerm.class });
          results = (Integer[]) RPC.call(removeDocuments, params, addresses
              .toArray(new InetSocketAddress[addresses.size()]), conf);
        } catch (SecurityException e) {
          e.printStackTrace();
        } catch (NoSuchMethodException e) {
          e.printStackTrace();
        }
        for (int k = 0; k < results.length; k++) {
          if (results[k] > 0) {
            result += results[k];
            shardLocation.put(shards[k], locations[k]);
          }
        }
      } else {
        result += getDataNode(getLocation(index).getAddress()).removeDocuments(
            index, new WTerm(term));
      }
      return result;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.dlucene.IIndexUpdater#commit()
     */
    public void commit() throws IOException {
      updateCache(false);
      for (String s : shardLocation.keySet()) {
        IndexLocation location = shardLocation.get(s);
        if (location != null) {
          ClientToDataNodeProtocol dataNode = getDataNode(location.getAddress());
          IndexVersion iv = dataNode.commitVersion(currentShard);
          IndexLocation nl = new IndexLocation(location.getAddress(), iv,
              IndexState.LIVE);
          addIndex(nl);
          LOG.debug("Committing Index " + currentShard + " " + nl);
        } else {
          throw new IOException("Index " + currentShard
              + " does not have any uncommitted changes");
        }
      }
    }
  }
}
