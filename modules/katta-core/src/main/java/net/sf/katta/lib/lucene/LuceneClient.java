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
package net.sf.katta.lib.lucene;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.katta.client.Client;
import net.sf.katta.client.ClientResult;
import net.sf.katta.client.INodeSelectionPolicy;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import com.google.common.collect.ImmutableList;

/**
 * Default implementation of {@link ILuceneClient}.
 */
public class LuceneClient implements ILuceneClient {

  protected final static Logger LOG = Logger.getLogger(LuceneClient.class);

  @SuppressWarnings("unused")
  private static Method getMethod(String name, Class<?>... parameterTypes) {
    try {
      return ILuceneServer.class.getMethod("search", parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method " + name + "(" + Arrays.asList(parameterTypes)
              + ") in ILuceneSearch!");
    }
  }

  private long _timeout = 12000;
  private Client _kattaClient;

  public LuceneClient() {
    _kattaClient = new Client(getServerClass());
  }

  public LuceneClient(final INodeSelectionPolicy nodeSelectionPolicy) {
    _kattaClient = new Client(getServerClass(), nodeSelectionPolicy);
  }

  public LuceneClient(InteractionProtocol protocol) {
    _kattaClient = new Client(getServerClass(), protocol);
  }

  public LuceneClient(final ZkConfiguration zkConfig) {
    _kattaClient = new Client(getServerClass(), zkConfig);
  }

  public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig) {
    _kattaClient = new Client(getServerClass(), policy, zkConfig);
  }

  public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig,
          ClientConfiguration clientConfiguration) {
    _kattaClient = new Client(getServerClass(), policy, zkConfig, clientConfiguration);
  }

  public Client getClient() {
    return _kattaClient;
  }

  public long getTimeout() {
    return _timeout;
  }

  public void setTimeout(long timeout) {
    this._timeout = timeout;
  }

  @Override
  public Hits search(final Query query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  private static final Method SEARCH_METHOD;
  private static final Method SORTED_SEARCH_METHOD;
  private static final Method FILTERED_SEARCH_METHOD;
  private static final Method FILTERED_SORTED_SEARCH_METHOD;
  private static final int SEARCH_METHOD_SHARD_ARG_IDX = 2;
  static {
    try {
      SEARCH_METHOD = ILuceneServer.class.getMethod("search", new Class[] { QueryWritable.class,
              DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method search() in ILuceneSearch!");
    }
    try {
      SORTED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", new Class[] { QueryWritable.class,
              DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, SortWritable.class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method search() in ILuceneSearch!");
    }
    try {
      FILTERED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", new Class[] { QueryWritable.class,
              DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, FilterWritable.class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method search() in ILuceneSearch!");
    }
    try {
      FILTERED_SORTED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", new Class[] { QueryWritable.class,
              DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, SortWritable.class,
              FilterWritable.class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method search() in ILuceneSearch!");
    }
  }

  @Override
  public Hits search(final Query query, final String[] indexNames, final int count) throws KattaException {
    return search(query, indexNames, count, null, null);
  }

  @Override
  public Hits search(final Query query, final String[] indexNames, final int count, final Sort sort)
          throws KattaException {
    return search(query, indexNames, count, sort, null);
  }

  @Override
  public Hits search(final Query query, final String[] indexNames, final int count, final Sort sort, final Filter filter)
          throws KattaException {
    final DocumentFrequencyWritable docFreqs = getDocFrequencies(query, indexNames);
    ClientResult<HitsMapWritable> results;

    if (sort == null && filter == null) {
      results = _kattaClient.broadcastToIndices(_timeout, true, SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX, indexNames,
              new QueryWritable(query), docFreqs, null, _timeout, Integer.valueOf(count));
    } else if (sort != null && filter == null) {
      results = _kattaClient.broadcastToIndices(_timeout, true, SORTED_SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX,
              indexNames, new QueryWritable(query), docFreqs, null, _timeout, Integer.valueOf(count), new SortWritable(
                      sort));
    } else if (sort == null && filter != null) {
      results = _kattaClient.broadcastToIndices(_timeout, true, FILTERED_SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX,
              indexNames, new QueryWritable(query), docFreqs, null, _timeout, Integer.valueOf(count),
              new FilterWritable(filter));
    } else {
      results = _kattaClient.broadcastToIndices(_timeout, true, FILTERED_SORTED_SEARCH_METHOD,
              SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, _timeout,
              Integer.valueOf(count), new SortWritable(sort), new FilterWritable(filter));
    }
    if (results.isError()) {
      throw results.getKattaException();
    }
    Hits result = new Hits();
    HitsMapWritable exampleHitWritable = null;
    if (!results.getMissingShards().isEmpty()) {
      LOG.warn("incomplete result - missing shard-results: " + results.getMissingShards() + ", "
              + results.getShardCoverage());
      result.setMissingShards(results.getMissingShards());
    }
    for (HitsMapWritable hmw : results.getResults()) {
      List<Hit> hits = hmw.getHitList();
      if (exampleHitWritable == null && !hits.isEmpty()) {
        exampleHitWritable = hmw;
      }
      result.addTotalHits(hmw.getTotalHits());
      result.addHits(hits);
    }
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    if (result.size() > 0) {
      if (sort == null) {
        result.sort(count);
      } else {
        result.fieldSort(sort, exampleHitWritable.getSortFieldTypes(), count);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time for sorting: " + (System.currentTimeMillis() - start) + " ms");
    }
    return result;
  }

  private static final Method COUNT_METHOD;
  private static final Method FILTER_COUNT_METHOD;
  private static final int COUNT_METHOD_SHARD_ARG_IDX = 1;
  private static final int FILTER_COUNT_METHOD_SHARD_ARG_IDX = 2;
  static {
    try {
      COUNT_METHOD = ILuceneServer.class.getMethod("getResultCount", new Class[] { QueryWritable.class, String[].class,
              Long.TYPE });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getResultCount() in ILuceneSearch!");
    }
    try {
      FILTER_COUNT_METHOD = ILuceneServer.class.getMethod("getResultCount", new Class[] { QueryWritable.class,
              FilterWritable.class, String[].class, Long.TYPE });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getResultCount() in ILuceneSearch!");
    }
  }

  @Override
  public int count(final Query query, final String[] indexNames) throws KattaException {
    ClientResult<Integer> results = _kattaClient.broadcastToIndices(_timeout, true, COUNT_METHOD,
            COUNT_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), null, _timeout);
    if (results.isError()) {
      throw results.getKattaException();
    }
    int count = 0;
    for (Integer n : results.getResults()) {
      count += n.intValue();
    }
    return count;
  }

  @Override
  public int count(final Query query, Filter filter, final String[] indexNames) throws KattaException {
    ClientResult<Integer> results = _kattaClient.broadcastToIndices(_timeout, true, FILTER_COUNT_METHOD,
            FILTER_COUNT_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), new FilterWritable(filter), null,
            _timeout);
    if (results.isError()) {
      throw results.getKattaException();
    }
    int count = 0;
    for (Integer n : results.getResults()) {
      count += n.intValue();
    }
    return count;
  }

  private static final Method DOC_FREQ_METHOD;
  private static final int DOC_FREQ_METHOD_SHARD_ARG_IDX = 1;
  static {
    try {
      DOC_FREQ_METHOD = ILuceneServer.class.getMethod("getDocFreqs",
              new Class[] { QueryWritable.class, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getDocFreqs() in ILuceneSearch!");
    }
  }

  protected DocumentFrequencyWritable getDocFrequencies(final Query query, final String[] indexNames)
          throws KattaException {
    ClientResult<DocumentFrequencyWritable> results = _kattaClient.broadcastToIndices(_timeout, true, DOC_FREQ_METHOD,
            DOC_FREQ_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), null);
    if (results.isError()) {
      throw results.getKattaException();
    }
    DocumentFrequencyWritable result = null;
    for (DocumentFrequencyWritable df : results.getResults()) {
      if (result == null) {
        // Start with first result.
        result = df;
      } else {
        // Aggregate rest of results into first result.
        result.addNumDocs(df.getNumDocs());
        result.putAll(df.getAll());
      }
    }
    if (result == null) {
      result = new DocumentFrequencyWritable(); // TODO: ?
    }
    return result;
  }

  /*
   * public MapWritable getDetails(String[] shards, int docId, String[] fields)
   * throws IOException; public MapWritable getDetails(String[] shards, int
   * docId) throws IOException;
   */
  private static final Method GET_DETAILS_METHOD;
  private static final Method GET_DETAILS_FIELDS_METHOD;
  private static final int GET_DETAILS_METHOD_SHARD_ARG_IDX = 0;
  private static final int GET_DETAILS_FIELDS_METHOD_SHARD_ARG_IDX = 0;
  static {
    try {
      GET_DETAILS_METHOD = ILuceneServer.class.getMethod("getDetails", new Class[] { String[].class, MapWritable.class });
      GET_DETAILS_FIELDS_METHOD = ILuceneServer.class.getMethod("getDetails", new Class[] { String[].class,
          MapWritable.class, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getDetails() in ILuceneSearch!");
    }
  }

  @Override
  public MapWritable getDetails(final Hit hit) throws KattaException {
    return getDetails(hit, null);
  }

  @Override
  public MapWritable getDetails(final Hit hit, final String[] fields) throws KattaException {
    String shardName = hit.getShard();
    Map<String, List<MapWritable>> rpcResult = getDetails(Collections.singletonMap(shardName, Collections.singletonList(hit.getDocId())), fields);
    
    MapWritable result = null;
    if (rpcResult.size() != 0) {
      List<MapWritable> shardResult = rpcResult.get(shardName);
      if (shardResult.size() != 0) {
        result = shardResult.get(0);
      }
    }
    return result;
  }
  
  private Map<String, List<MapWritable>> getDetails(Map<String, List<Integer>> docIdsByShard, String[] fields) throws KattaException {
    MapWritable docsArg = new MapWritable();
    List<String> shards = new ArrayList<String>();
    
    for (Map.Entry<String, List<Integer>> entry : docIdsByShard.entrySet()) {
      List<Integer> docIds = entry.getValue();
      IntWritable[] docIdsWritable = new IntWritable[docIds.size()];
      int i = 0;
      for (Integer docId : docIds) {
        docIdsWritable[i++] = new IntWritable(docId);
      }
      String shardName = entry.getKey();
      docsArg.put(new Text(shardName), new IntArrayWritable(docIdsWritable));
      shards.add(shardName);
    }
    
    Object[] args;
    Method method;
    int shardArgIdx;
    if (fields == null) {
      args = new Object[] { null, docsArg };
      method = GET_DETAILS_METHOD;
      shardArgIdx = GET_DETAILS_METHOD_SHARD_ARG_IDX;
    } else {
      args = new Object[] { null, docsArg, fields};
      method = GET_DETAILS_FIELDS_METHOD;
      shardArgIdx = GET_DETAILS_FIELDS_METHOD_SHARD_ARG_IDX;
    }
    ClientResult<MapWritable> rpcResult = _kattaClient.broadcastToShards(_timeout, true, method, shardArgIdx, shards,
        args);
    
    Map<String, List<MapWritable>> resultMap = new HashMap<String, List<MapWritable>>();
    
    for (MapWritable mapWritable : rpcResult.getResultsOrThrowKattaException()) {
      for (Entry<Writable, Writable> entry : mapWritable.entrySet()) {
        String shardName = ((Text)entry.getKey()).toString();
        Writable[] docs = (Writable[]) ((ArrayWritable) entry.getValue()).get();
        
        resultMap.put(shardName, (List)Arrays.asList(docs));
      }
    }
    
    return resultMap;
  }

  @Override
  public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException {
    return getDetails(hits, null);
  }

  @Override
  public List<MapWritable> getDetails(List<Hit> hits, final String[] fields) throws KattaException,
          InterruptedException {
    Map<String, List<Integer>> docIdsByShard = new HashMap<String, List<Integer>>();
    Map<String, List<Integer>> resultPosByShard = new HashMap<String, List<Integer>>();
    
    int nextPos = 0;
    for (Hit hit : hits) {
      String shardName = hit.getShard();
      List<Integer> shardDocIds = docIdsByShard.get(shardName);
      List<Integer> shardResultPositions;
      if (shardDocIds == null) {
        shardDocIds = new ArrayList<Integer>();
        docIdsByShard.put(shardName, shardDocIds);
        
        shardResultPositions = new ArrayList<Integer>();
        resultPosByShard.put(shardName, shardResultPositions);
      } else {
        shardResultPositions = resultPosByShard.get(shardName);
      }
      
      shardDocIds.add(hit.getDocId());
      shardResultPositions.add(nextPos++);
    }
    
    Map<String, List<MapWritable>> resultsByShard = getDetails(docIdsByShard, fields);
    MapWritable[] result = new MapWritable[hits.size()];
    for (Entry<String, List<MapWritable>> entry : resultsByShard.entrySet()) {
      String shardName = entry.getKey();
      
      List<Integer> shardResultPos = resultPosByShard.get(shardName);
      Iterator<Integer> shardResultPosIter = shardResultPos.iterator();
      
      List<MapWritable> shardResults = entry.getValue();
      Iterator<MapWritable> shardResultsIter = shardResults.iterator();
      
      while (shardResultPosIter.hasNext() && shardResultsIter.hasNext()) {
        Integer pos = shardResultPosIter.next();
        MapWritable doc = shardResultsIter.next();
        
        result[pos] = doc;
      }
      
      if (shardResultPosIter.hasNext() != shardResultsIter.hasNext()) {
        throw new KattaException(String.format("mismatched results returned from shard %s: expected %d, got %d", shardName, shardResultPos.size(), shardResults.size()));
      }
    }
    
    return Arrays.asList(result);
  }

  @Override
  public double getQueryPerMinute() {
    return _kattaClient.getQueryPerMinute();
  }

  @Override
  public void close() {
    _kattaClient.close();
  }

  protected Client getKattaClient() {
    return _kattaClient;
  }

  protected Class<? extends ILuceneServer> getServerClass() {
    return ILuceneServer.class;
  }
}
