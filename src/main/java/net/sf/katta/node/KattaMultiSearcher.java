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
package net.sf.katta.node;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implements search over a set of named <code>Searchables</code>.
 * 
 */
public class KattaMultiSearcher {

  private final static Logger LOG = Logger.getLogger(KattaMultiSearcher.class);

  private final Map<String, IndexSearcher> _searchers = new ConcurrentHashMap<String, IndexSearcher>();
  private ExecutorService _threadPool = Executors.newFixedThreadPool(100);

  private final String _node;

  //TODO is this needed?  It is never used.
  private int _maxDoc = 0;

  public KattaMultiSearcher(final String node) {
    _node = node;
  }

  /**
   * Adds an shard index search for given name to the list of shards
   * MultiSearcher search in.
   * 
   * @param shardKey
   * @param indexSearcher
   * @throws IOException
   */
  public void addShard(final String shardKey, final IndexSearcher indexSearcher) throws IOException {
    synchronized (_searchers) {
      _searchers.put(shardKey, indexSearcher);
      _maxDoc += indexSearcher.maxDoc();
    }
  }

  /**
   * 
   * Removes a search by given shardName from the list of searchers.
   */
  public void removeShard(final String shardName) {
    synchronized (_searchers) {
      final Searchable remove = _searchers.remove(shardName);
      if (remove == null) {
        return; // nothing to do.
      }
      try {
        _maxDoc -= remove.maxDoc();
      } catch (final IOException e) {
        throw new RuntimeException("unable to retrive maxDocs from searchable");
      }
    }

  }

  /**
   * Search in the given shards and return max hits for given query
   * 
   * @param query
   * @param freqs
   * @param shards
   * @param result
   * @param max
   * @throws IOException
   */
  public final void search(final Query query, final DocumentFrequencyWritable freqs, final String[] shards,
      final HitsMapWritable result, final int max) throws IOException {
    final Query rewrittenQuery = rewrite(query, shards);
    final int numDocs = freqs.getNumDocs();
    final Weight weight = rewrittenQuery.weight(new CachedDfSource(freqs.getAll(), numDocs, new DefaultSimilarity()));
    // limit the request to the number requested or the total number of documents, whichever is smaller
    final int limit = Math.min(numDocs, max);
    final KattaHitQueue hq = new KattaHitQueue(limit);
    int totalHits = 0;
    final int shardsCount = shards.length;

    // run the search in parallel on the shards with a thread pool
    List<Future<SearchResult>> tasks = new ArrayList<Future<SearchResult>>();
    for (int i = 0; i < shardsCount; i++) {
      SearchCall call = new SearchCall(shards[i], weight, limit);
      Future<SearchResult> future = _threadPool.submit(call);
      tasks.add(future);
    }

    final ScoreDoc[][] scoreDocs = new ScoreDoc[shardsCount][];
    for (int i = 0; i < shardsCount; i++) {
      SearchResult searchResult;
      try {
        searchResult = tasks.get(i).get();
        totalHits += searchResult._totalHits;
        scoreDocs[i] = searchResult._scoreDocs;
      } catch (InterruptedException e) {
        throw new IOException("Multithread shard search interrupted:", e);
      } catch (ExecutionException e) {
        throw new IOException("Multithread shard search could not be executed:", e);
      }
    }
   
    result.addTotalHits(totalHits);

    int pos = 0;
    BitSet done = new BitSet(shardsCount);
    while (done.cardinality() != shardsCount) {
      ScoreDoc scoreDoc = null;
      for (int i = 0; i < shardsCount; i++) {
        // only process this shard if it is not yet done.
        if (!done.get(i)) {
          final ScoreDoc[] docs = scoreDocs[i];
          if (pos < docs.length) {
            scoreDoc = docs[pos];
            final Hit hit = new Hit(shards[i], _node, scoreDoc.score, scoreDoc.doc);
            if (!hq.insert(hit)) {
              // no doc left that has a higher score than the lowest score in
              // the queue
              done.set(i, true);
            }
          } else {
            // no docs left in this shard
            done.set(i, true);
          }
        }
      }
      // we always wait until we got all hits from this position in all shards.
      
      
      pos++;
      if (scoreDoc == null) {
        // we do not have any more data
        break;
      }
    }

    for (Hit hit : hq) {
      if (hit != null) {
        result.addHitToShard(hit.getShard(), hit);
      }
    }
  }

  /**
   * Returns the number of documents a shard has.
   * 
   * @param shardName
   * @return
   */
  public int getNumDoc(final String shardName) {
    final Searchable searchable = _searchers.get(shardName);
    if (searchable != null) {
      final IndexSearcher indexSearcher = (IndexSearcher) searchable;
      return indexSearcher.getIndexReader().numDocs();
    }
    throw new IllegalArgumentException("shard " + shardName + " unknown");
  }

  /**
   * Returns a specified lucene document from a given shard.
   * 
   * @param shardName
   * @param docId
   * @return
   * @throws CorruptIndexException
   * @throws IOException
   */
  public Document doc(final String shardName, final int docId) throws IOException {
    final Searchable searchable = _searchers.get(shardName);
    if (searchable != null) {
      return searchable.doc(docId);
    }
    throw new IllegalArgumentException("shard " + shardName + " unknown");
  }

  /**
   * Rewrites a query for the given shards
   * 
   * @param original
   * @param shardNames
   * @return
   * @throws IOException
   */
  public Query rewrite(final Query original, final String[] shardNames) throws IOException {
    final Query[] queries = new Query[shardNames.length];
    for (int i = 0; i < shardNames.length; i++) {
      final String shard = shardNames[i];
      queries[i] = _searchers.get(shard).rewrite(original);
    }
    if (queries.length > 0) {
      return queries[0].combine(queries);
    }
    return original;
  }

  /**
   * Returns the document frequency for a given term within a given shard.
   * 
   * @param shardName
   * @param term
   * @return
   * @throws IOException
   */
  public int docFreq(final String shardName, final Term term) throws IOException {
    int result = 0;
    final Searchable searchable = _searchers.get(shardName);
    if (searchable != null) {
      result = searchable.docFreq(term);
    } else {
      LOG.error("No shard with the name '" + shardName + "' on in this searcher.");
    }
    return result;
  }

  public void close() throws IOException {
    for (final Searchable searchable : _searchers.values()) {
      searchable.close();
    }
  }

  /**
   * Implements a single thread of a search.  Each shard has a separate SearchCall and they
   * are run more or less in parallel.
   */
  private class SearchCall implements Callable<SearchResult> {

    private final String _shardName;
    private final Weight _weight;
    private final int _limit;

    public SearchCall(String shardName, Weight weight, int limit) {
      _shardName = shardName;
      _weight = weight;
      _limit = limit;
    }

    @Override
    public SearchResult call() throws Exception {
      final IndexSearcher indexSearcher = _searchers.get(_shardName);
      final TopDocs docs = indexSearcher.search(_weight, null, _limit);
      return new SearchResult(docs.totalHits, docs.scoreDocs);
    }

  }

  private static class SearchResult {
    private final int _totalHits;
    private final ScoreDoc[] _scoreDocs;

    public SearchResult(int totalHits, ScoreDoc[] scoreDocs) {
      _totalHits = totalHits;
      _scoreDocs = scoreDocs;
    }

  }

  // cached document frequency source from apache lucene
  // MultiSearcher.
  /**
   * Document Frequency cache acting as a Dummy-Searcher. This class is not a
   * fully-fledged Searcher, but only supports the methods necessary to
   * initialize Weights.
   */
  private static class CachedDfSource extends Searcher {
    private final Map<TermWritable, Integer> dfMap; // Map from Terms to corresponding doc freqs

    private final int maxDoc; // document count

    public CachedDfSource(final Map<TermWritable, Integer> dfMap, final int maxDoc, final Similarity similarity) {
      this.dfMap = dfMap;
      this.maxDoc = maxDoc;
      setSimilarity(similarity);
    }

    @Override
    public int docFreq(final Term term) {
      int df;
      try {
        df = dfMap.get(new TermWritable(term.field(), term.text()));
      } catch (final NullPointerException e) {
        throw new IllegalArgumentException("df for term " + term.text() + " not available");
      }
      return df;
    }

    @Override
    public int[] docFreqs(final Term[] terms) {
      final int[] result = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        result[i] = docFreq(terms[i]);
      }
      return result;
    }

    @Override
    public int maxDoc() {
      return maxDoc;
    }

    @Override
    public Query rewrite(final Query query) {
      // this is a bit of a hack. We know that a query which
      // creates a Weight based on this Dummy-Searcher is
      // always already rewritten (see preparedWeight()).
      // Therefore we just return the unmodified query here
      return query;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Document doc(final int i) {
      throw new UnsupportedOperationException();
    }

    public Document doc(final int i, final FieldSelector fieldSelector) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Explanation explain(final Weight weight, final int doc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void search(final Weight weight, final Filter filter, final HitCollector results) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopDocs search(final Weight weight, final Filter filter, final int n) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopFieldDocs search(final Weight weight, final Filter filter, final int n, final Sort sort) {
      throw new UnsupportedOperationException();
    }
  }

  protected class KattaHitQueue extends PriorityQueue implements Iterable<Hit> {
    KattaHitQueue(final int size) {
      initialize(size);
    }

    @Override
    protected final boolean lessThan(final Object a, final Object b) {
      final Hit hitA = (Hit) a;
      final Hit hitB = (Hit) b;
      if (hitA.getScore() == hitB.getScore()) {
        // todo this of cource do not work since we have same shardKeys
        // (should we increment docIds?)
        return hitA.getDocId() > hitB.getDocId();
      }
      return hitA.getScore() < hitB.getScore();
    }

    public Iterator<Hit> iterator() {
      return new Iterator<Hit>() {
        public boolean hasNext() {
          return KattaHitQueue.this.size() > 0;
        }

        public Hit next() {
          return (Hit) KattaHitQueue.this.pop();
        }

        public void remove() {
          throw new UnsupportedOperationException("Can't remove using this iterator");
        }
      };
    }
  }

}
