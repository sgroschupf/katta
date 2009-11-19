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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.katta.node.DocumentFrequencyWritable;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.ILuceneServer;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.QueryWritable;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

/**
 * Default implementation of {@link ILuceneClient}.
 */
public class LuceneClient implements ILuceneClient {

  protected final static Logger LOG = Logger.getLogger(LuceneClient.class);
  private final static long TIMEOUT = 12000;

  @SuppressWarnings("unused")
  private static Method getMethod(String name, Class<?>... parameterTypes) {
    try {
      return ILuceneServer.class.getMethod("search", parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method " + name + "(" + Arrays.asList(parameterTypes)
              + ") in ILuceneSearch!");
    }
  }

  private Client kattaClient;

  public LuceneClient() {
    kattaClient = new Client(ILuceneServer.class);
  }

  public LuceneClient(final INodeSelectionPolicy nodeSelectionPolicy) {
    kattaClient = new Client(ILuceneServer.class, nodeSelectionPolicy);
  }

  public LuceneClient(final ZkConfiguration zkConfig) {
    kattaClient = new Client(ILuceneServer.class, zkConfig);
  }

  public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig) {
    kattaClient = new Client(ILuceneServer.class, policy, zkConfig);
  }

  public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig,
      ClientConfiguration clientConfiguration) {
    kattaClient = new Client(ILuceneServer.class, policy, zkConfig, clientConfiguration);
  }

  @Deprecated
  /*
   * @deprecated Old API uses IQuery, which just transports a string; also limited to using
   * a KeywordAnalyzer internally.
   */
  public Hits search(final IQuery query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  public Hits search(final Query query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  @Deprecated
  /*
   * @deprecated Old API uses IQuery, which just transports a string; also limited to using
   * a KeywordAnalyzer internally.
   */
  public Hits search(final IQuery query, final String[] indexNames, final int count) throws KattaException {
    try {
      final QueryParser luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());
      Query luceneQuery = luceneQueryParser.parse(query.getQuery());
      return search(luceneQuery, indexNames, count);
    } catch (ParseException e) {
      throw new KattaException("Unable to parse Query: " + query.getQuery(), e);
    }
  }

  private static final Method SEARCH_METHOD;
  private static final int SEARCH_METHOD_SHARD_ARG_IDX = 2;
  static {
    try {
      SEARCH_METHOD = ILuceneServer.class.getMethod("search", new Class[] { QueryWritable.class,
              DocumentFrequencyWritable.class, String[].class, Integer.TYPE });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method search() in ILuceneSearch!");
    }
  }

  public Hits search(final Query query, final String[] indexNames, final int count) throws KattaException {
    final DocumentFrequencyWritable docFreqs = getDocFrequencies(query, indexNames);
    ClientResult<HitsMapWritable> results = kattaClient.broadcastToIndices(TIMEOUT, true, SEARCH_METHOD,
            SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, Integer.valueOf(count));
    if (results.isError()) {
      throw results.getKattaException();
    }
    Hits result = new Hits();
    for (HitsMapWritable hmw : results.getResults()) {
      Hits hits = hmw.getHits();
      result.addTotalHits(hits.size());
      result.addHits(hits.getHits());
    }
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    result.sort(count);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time for sorting: " + (System.currentTimeMillis() - start) + " ms");
    }
    return result;
  }

  // public int getResultCount(QueryWritable query, String[] shards) throws
  // IOException;

  private static final Method COUNT_METHOD;
  private static final int COUNT_METHOD_SHARD_ARG_IDX = 1;
  static {
    try {
      COUNT_METHOD = ILuceneServer.class.getMethod("getResultCount",
              new Class[] { QueryWritable.class, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getResultCount() in ILuceneSearch!");
    }
  }

  @Deprecated
  public int count(final IQuery query, final String[] indexNames) throws KattaException {
    try {
      final QueryParser luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());
      Query luceneQuery = luceneQueryParser.parse(query.getQuery());
      return count(luceneQuery, indexNames);
    } catch (ParseException e) {
      throw new KattaException("Unable to parse Query: " + query.getQuery(), e);
    }
  }

  public int count(final Query query, final String[] indexNames) throws KattaException {
    ClientResult<Integer> results = kattaClient.broadcastToIndices(TIMEOUT, true, COUNT_METHOD,
            COUNT_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), null);
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

  private DocumentFrequencyWritable getDocFrequencies(final Query query, final String[] indexNames)
          throws KattaException {
    ClientResult<DocumentFrequencyWritable> results = kattaClient.broadcastToIndices(TIMEOUT, true, DOC_FREQ_METHOD,
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
      GET_DETAILS_METHOD = ILuceneServer.class.getMethod("getDetails", new Class[] { String[].class, Integer.TYPE });
      GET_DETAILS_FIELDS_METHOD = ILuceneServer.class.getMethod("getDetails", new Class[] { String[].class,
              Integer.TYPE, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method getDetails() in ILuceneSearch!");
    }
  }

  public MapWritable getDetails(final Hit hit) throws KattaException {
    return getDetails(hit, null);
  }

  public MapWritable getDetails(final Hit hit, final String[] fields) throws KattaException {
    List<String> shards = new ArrayList<String>();
    shards.add(hit.getShard());
    int docId = hit.getDocId();
    //
    Object[] args;
    Method method;
    int shardArgIdx;
    if (fields == null) {
      args = new Object[] { null, Integer.valueOf(docId) };
      method = GET_DETAILS_METHOD;
      shardArgIdx = GET_DETAILS_METHOD_SHARD_ARG_IDX;
    } else {
      args = new Object[] { null, Integer.valueOf(docId), fields };
      method = GET_DETAILS_FIELDS_METHOD;
      shardArgIdx = GET_DETAILS_FIELDS_METHOD_SHARD_ARG_IDX;
    }
    ClientResult<MapWritable> results = kattaClient.broadcastToShards(TIMEOUT, true, method, shardArgIdx, shards, args);
    if (results.isError()) {
      throw results.getKattaException();
    }
    return results.getResults().isEmpty() ? null : results.getResults().iterator().next();
  }

  public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException {
    return getDetails(hits, null);
  }

  public List<MapWritable> getDetails(List<Hit> hits, final String[] fields) throws KattaException,
          InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(Math.min(10, hits.size() + 1));
    List<MapWritable> results = new ArrayList<MapWritable>();
    List<Future<MapWritable>> futures = new ArrayList<Future<MapWritable>>();
    for (final Hit hit : hits) {
      futures.add(executorService.submit(new Callable<MapWritable>() {
        public MapWritable call() throws Exception {
          return getDetails(hit, fields);
        }
      }));
    }

    for (Future<MapWritable> future : futures) {
      try {
        results.add(future.get());
      } catch (ExecutionException e) {
        throw new KattaException("Could not get hit details.", e.getCause());
      }
    }

    executorService.shutdown();

    return results;
  }

  public double getQueryPerMinute() {
    return kattaClient.getQueryPerMinute();
  }

  public void close() {
    kattaClient.close();
  }

}
