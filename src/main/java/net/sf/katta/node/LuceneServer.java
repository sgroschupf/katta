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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.katta.client.lucene.FieldSortComparator;
import net.sf.katta.util.WritableType;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.PriorityQueue;

/**
 * The back end server which searches a set of Lucene indices.
 * Each shard is a Lucene index directory.
 * <p>
 * Normal usage is to first call getDocFreqs() to get the global
 * term frequencies, then pass that back in to search(). This way
 * you get uniform scoring across all the nodes / instances of 
 * LuceneServer.
 */
public class LuceneServer implements INodeManaged, ILuceneServer {

  private final static Logger LOG = Logger.getLogger(LuceneServer.class);

  protected final Map<String, IndexSearcher> _searchers = new ConcurrentHashMap<String, IndexSearcher>();
  protected ExecutorService _threadPool = Executors.newFixedThreadPool(100);

  protected String _nodeName;
  protected int _maxDoc = 0;

  public LuceneServer() {
    // do nothing
  }

  public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
    return 0L;
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  /**
   * Adds an shard index search for given name to the list of shards
   * MultiSearcher search in.
   * 
   * @param shardName
   * @param shardDir
   * @throws IOException
   */
  public void addShard(final String shardName, final File shardDir) throws IOException {
    LOG.info("LuceneServer " + _nodeName + " got shard " + shardName);
    try {
      IndexSearcher indexSearcher = new IndexSearcher(shardDir.getAbsolutePath());
      synchronized (_searchers) {
        _searchers.put(shardName, indexSearcher);
        _maxDoc += indexSearcher.maxDoc();
      }
    } catch (CorruptIndexException e) {
      LOG.error("Error building index for shard " + shardName, e);
      throw e;
    }
  }

  /**
   * Removes a search by given shardName from the list of searchers.
   */
  public void removeShard(final String shardName) {
    LOG.info("LuceneServer " + _nodeName + " removing shard " + shardName);
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
   * Returns the number of documents a shard has.
   * 
   * @param shardName
   * @return the number of documents in the shard.
   */
  protected int shardSize(String shardName) {
    final Searchable searchable = _searchers.get(shardName);
    if (searchable != null) {
      final IndexSearcher indexSearcher = (IndexSearcher) searchable;
      int size = indexSearcher.getIndexReader().numDocs();
      LOG.debug("Shard " + shardName + " has " + size + " docs.");
      return size;
    } else {
      throw new IllegalArgumentException("Shard " + shardName + " unknown");
    }
  }
  
  /**
   * Returns data about a shard. Currently the only standard key is
   * SHARD_SIZE_KEY. This value will be reported by the listIndexes command.
   * The units depend on the type of server. It is OK to return an empty
   * map or null.
   * 
   * @param shardName The name of the shard to measure. 
   * This was the name provided in addShard().
   * @return a map of key/value pairs which describe the shard.
   * @throws Exception 
   */
  public Map<String, String> getShardMetaData(String shardName) throws Exception {
    Map<String, String> metaData = new HashMap<String, String>();
    metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
    return metaData;
  }

  /**
   * Close all Lucene indices. No further calls will be made after this one.
   */
  public void shutdown() throws IOException {
    for (final Searchable searchable : _searchers.values()) {
      searchable.close();
    }
    _searchers.clear();
  }


  /**
   * Returns all Hits that match the query. This might be significant slower as
   * {@link #search(QueryWritable, DocumentFrequencyWritable , String[], int)} since we
   * replace count with Integer.MAX_VALUE.
   * 
   * @param query         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shardNames) throws IOException {
    return search(query, freqs, shardNames, Integer.MAX_VALUE);
  }


  /**
   * @param query         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param count         The top n high score hits.
   * @return A list of hits from the search.
   * @throws ParseException  If the query is ill-formed.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(final QueryWritable query, final DocumentFrequencyWritable freqs,
      final String[] shards, final int count) throws IOException {
    return search(query, freqs, shards, count, null);
  }

  @Override
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shards, int count,
      SortWritable sortWritable) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("You are searching with the query: '" + query.getQuery() + "'");
    }

    Query luceneQuery = query.getQuery();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Lucene query: " + luceneQuery.toString());
    }

    long completeSearchTime = 0;
    final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(_nodeName);
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    Sort sort = null;
    if (sortWritable != null) {
      sort = sortWritable.getSort();
    }
    search(luceneQuery, freqs, shards, result, count, sort);
    if (LOG.isDebugEnabled()) {
      final long end = System.currentTimeMillis();
      LOG.debug("Search took " + (end - start) / 1000.0 + "sec.");
      completeSearchTime += (end - start);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
      final DataOutputBuffer buffer = new DataOutputBuffer();
      result.write(buffer);
      LOG.debug("Result size to transfer: " + buffer.getLength());
    }
    return result;
  }


  /**
   * Returns the number of documents a term occurs in. In a distributed search
   * environment, we need to get this first and then query all nodes again with
   * this information to ensure we compute TF IDF correctly. See
   * {@link http://lucene.apache.org/java/2_3_0/api/org/apache/lucene/search/Similarity.html}
   * 
   * @param input       TODO is this really just a Lucene query?
   * @param shards      The shards to search in.
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public DocumentFrequencyWritable getDocFreqs(final QueryWritable input, final String[] shards) throws IOException {
    Query luceneQuery = input.getQuery();

    final Query rewrittenQuery = rewrite(luceneQuery, shards);
    final DocumentFrequencyWritable docFreqs = new DocumentFrequencyWritable();

    final HashSet<Term> termSet = new HashSet<Term>();
    rewrittenQuery.extractTerms(termSet);
    int numDocs = 0;
    for (final String shard : shards) {
      final java.util.Iterator<Term> termIterator = termSet.iterator();
      while (termIterator.hasNext()) {
        final Term term = termIterator.next();
        final int docFreq = docFreq(shard, term);
        docFreqs.put(term.field(), term.text(), docFreq);
      }
      numDocs += shardSize(shard);
    }
    docFreqs.setNumDocs(numDocs);
    return docFreqs;
  }


  /**
   * Returns the lucene document. Each field:value tuple of the lucene document
   * is inserted into the returned map. In most cases
   * {@link #getDetails(String[], int, String[])} would be a better choice for
   * performance reasons.
   * 
   * @param shards       The shards to ask for the document.
   * @param docId        The document that is desired.
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public MapWritable getDetails(final String[] shards, final int docId) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = doc(shards[0], docId);
    final List<Fieldable> fields = doc.getFields();
    for (final Fieldable field : fields) {
      final String name = field.name();
      if (field.isBinary()) {
        final byte[] binaryValue = field.binaryValue();
        result.put(new Text(name), new BytesWritable(binaryValue));
      } else {
        final String stringValue = field.stringValue();
        result.put(new Text(name), new Text(stringValue));
      }
    }
    return result;
  }

  
  /**
   * Returns only the requested fields of a lucene document.  The fields are returned
   * as a map.
   * 
   * @param shard        The shard to ask for the document.
   * @param docId        The document that is desired.
   * @param fields       The fields to return.
   * @return             TODO what does this return?  A map?
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public MapWritable getDetails(final String[] shards, final int docId, final String[] fieldNames) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = doc(shards[0], docId);
    for (final String fieldName : fieldNames) {
      final Field field = doc.getField(fieldName);
      if (field != null) {
        if (field.isBinary()) {
          final byte[] binaryValue = field.binaryValue();
          result.put(new Text(fieldName), new BytesWritable(binaryValue));
        } else {
          final String stringValue = field.stringValue();
          result.put(new Text(fieldName), new Text(stringValue));
        }
      }
    }
    return result;
  }

  
  /**
   * Returns the number of documents that match the given query. This the
   * fastest way in case you just need the number of documents. Note that the
   * number of matching documents is also included in HitsMapWritable.
   * 
   * @param query
   * @param shards
   * @return
   * @throws IOException
   */
  public int getResultCount(final QueryWritable query, final String[] shards) throws IOException {
    final DocumentFrequencyWritable docFreqs = getDocFreqs(query, shards);
    return search(query, docFreqs, shards, 1).getTotalHits();
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
  protected final void search(final Query query, final DocumentFrequencyWritable freqs, final String[] shards,
      final HitsMapWritable result, final int max, Sort sort) throws IOException {
    final Query rewrittenQuery = rewrite(query, shards);
    final int numDocs = freqs.getNumDocs();
    final Weight weight = rewrittenQuery.weight(new CachedDfSource(freqs.getAll(), numDocs, new DefaultSimilarity()));
    // Limit the request to the number requested or the total number of documents, whichever is smaller.
    final int limit = Math.min(numDocs, max);
    int totalHits = 0;
    final int shardsCount = shards.length;

    // Run the search in parallel on the shards with a thread pool.
    List<Future<SearchResult>> tasks = new ArrayList<Future<SearchResult>>();
    for (int i = 0; i < shardsCount; i++) {
      SearchCall call = new SearchCall(shards[i], weight, limit, sort);
      Future<SearchResult> future = _threadPool.submit(call);
      tasks.add(future);
    }

    final ScoreDoc[][] scoreDocs = new ScoreDoc[shardsCount][];
    ScoreDoc scoreDocExample = null;
    for (int i = 0; i < shardsCount; i++) {
      SearchResult searchResult;
      try {
        searchResult = tasks.get(i).get();
        totalHits += searchResult._totalHits;
        scoreDocs[i] = searchResult._scoreDocs;
        if (scoreDocExample == null && scoreDocs[i].length > 0) {
          scoreDocExample = scoreDocs[i][0];
        }
      } catch (InterruptedException e) {
        throw new IOException("Multithread shard search interrupted:", e);
      } catch (ExecutionException e) {
        throw new IOException("Multithread shard search could not be executed:", e);
      }
    }

    result.addTotalHits(totalHits);

    final Iterable<Hit> finalHitList;
    if (sort == null || totalHits == 0) {
      final KattaHitQueue hq = new KattaHitQueue(limit);
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
              final Hit hit = new Hit(shards[i], _nodeName, scoreDoc.score, scoreDoc.doc);
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
        // we always wait until we got all hits from this position in all
        // shards.

        pos++;
        if (scoreDoc == null) {
          // we do not have any more data
          break;
        }
      }
      finalHitList = hq;
    } else {
      WritableType[] sortFieldsTypes = null;
      FieldDoc fieldDoc = (FieldDoc) scoreDocExample;
      sortFieldsTypes = WritableType.detectWritableTypes(fieldDoc.fields);
      result.setSortFieldTypes(sortFieldsTypes);
      finalHitList = mergeFieldSort(new FieldSortComparator(sort.getSort(), sortFieldsTypes), limit, scoreDocs, shards,
          _nodeName);
    }

    for (Hit hit : finalHitList) {
      if (hit != null) {
        result.addHitToShard(hit.getShard(), hit);
      }
    }
  }

  /**
   * Merges the already sorted sub-lists to one big sorted list.
   */
  private final static List<Hit> mergeFieldSort(FieldSortComparator comparator, int count,
      ScoreDoc[][] sortedFieldDocs, String[] shards, String nodeName) {
    int[] arrayPositions = new int[sortedFieldDocs.length];
    final List<Hit> sortedResult = new ArrayList<Hit>(count);

    BitSet listDone = new BitSet(sortedFieldDocs.length);
    do {
      int fieldDocArrayWithSmallestFieldDoc = -1;
      FieldDoc smallestFieldDoc = null;
      for (int subListIndex = 0; subListIndex < arrayPositions.length; subListIndex++) {
        if (!listDone.get(subListIndex)) {
          FieldDoc hit = (FieldDoc) sortedFieldDocs[subListIndex][arrayPositions[subListIndex]];
          if (smallestFieldDoc == null || comparator.compare(hit.fields, smallestFieldDoc.fields) < 0) {
            smallestFieldDoc = hit;
            fieldDocArrayWithSmallestFieldDoc = subListIndex;
          }
        }
      }
      ScoreDoc[] smallestElementList = sortedFieldDocs[fieldDocArrayWithSmallestFieldDoc];
      FieldDoc fieldDoc = (FieldDoc) smallestElementList[arrayPositions[fieldDocArrayWithSmallestFieldDoc]];
      arrayPositions[fieldDocArrayWithSmallestFieldDoc]++;
      final Hit hit = new Hit(shards[fieldDocArrayWithSmallestFieldDoc], nodeName, fieldDoc.score, fieldDoc.doc);
      hit.setSortFields(WritableType.convertComparable(comparator.getFieldTypes(), fieldDoc.fields));
      sortedResult.add(hit);
      if (arrayPositions[fieldDocArrayWithSmallestFieldDoc] >= smallestElementList.length) {
        listDone.set(fieldDocArrayWithSmallestFieldDoc, true);
      }
    } while (sortedResult.size() < count && listDone.cardinality() < arrayPositions.length);
    return sortedResult;
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
  protected Document doc(final String shardName, final int docId) throws IOException {
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
  protected Query rewrite(final Query original, final String[] shardNames) throws IOException {
    final Query[] queries = new Query[shardNames.length];
    for (int i = 0; i < shardNames.length; i++) {
      final String shard = shardNames[i];
      final IndexSearcher searcher = _searchers.get(shard);
      if (searcher == null) {
        LOG.error("Node " + _nodeName + ": unknown shard " + shard);
        //TODO PVo should we throw an exception here?
      } else {
        queries[i] = searcher.rewrite(original);
      }
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
  protected int docFreq(final String shardName, final Term term) throws IOException {
    int result = 0;
    final Searchable searchable = _searchers.get(shardName);
    if (searchable != null) {
      result = searchable.docFreq(term);
    } else {
      LOG.error("No shard with the name '" + shardName + "' on in this searcher.");
    }
    return result;
  }

  /**
   * Implements a single thread of a search.  Each shard has a separate SearchCall and they
   * are run more or less in parallel.
   */
  private class SearchCall implements Callable<SearchResult> {

    private final String _shardName;
    private final Weight _weight;
    private final int _limit;
    private final Sort _sort;

    public SearchCall(String shardName, Weight weight, int limit, Sort sort) {
      _shardName = shardName;
      _weight = weight;
      _limit = limit;
      _sort = sort;
    }

    @Override
    public SearchResult call() throws Exception {
      final IndexSearcher indexSearcher = _searchers.get(_shardName);
      final TopDocs docs;
      if (_sort != null) {
        docs = indexSearcher.search(_weight, null, _limit, _sort);
      } else {
        docs = indexSearcher.search(_weight, null, _limit);
      }
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

  // Cached document frequency source from apache lucene
  // MultiSearcher.
  /**
   * Document Frequency cache acting as a Dummy-Searcher. This class is not a
   * fully-fledged Searcher, but only supports the methods necessary to
   * initialize Weights.
   */
  private static class CachedDfSource extends Searcher {

    private final Map<TermWritable, Integer> dfMap; // Map from Terms to corresponding doc freqs.

    private final int maxDoc; // Document count.

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
        return hitA.getDocId() < hitB.getDocId();
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
