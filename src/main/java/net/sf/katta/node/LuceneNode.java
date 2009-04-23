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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * Implementation of a node serving lucene shards.
 * 
 */
public class LuceneNode extends BaseNode implements ISearch {

  public static final String NUM_OF_DOCS = "numOfDocs";

  private KattaMultiSearcher _searcher;

  public LuceneNode(ZKClient zkClient, NodeConfiguration configuration) {
    super(zkClient, configuration);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.katta.node.IRequestHandler#handle(org.apache.hadoop.io.Writable)
   */
  @Override
  public Writable handle(Writable request) {
    // TODO Auto-generated method stub
    return null;
  }

  public int getResultCount(final QueryWritable query, final String[] shards) throws IOException {
    final DocumentFrequenceWritable docFreqs = getDocFreqs(query, shards);
    return search(query, docFreqs, shards, 1).getTotalHits();
  }

  public HitsMapWritable search(final QueryWritable query, final DocumentFrequenceWritable freqs,
          final String[] shards, final int count) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("You are searching with the query: '" + query.getQuery() + "'");
    }

    Query luceneQuery = query.getQuery();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Lucene query: " + luceneQuery.toString());
    }

    long completeSearchTime = 0;
    final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(getName());
    if (_searcher != null) {
      long start = 0;
      if (LOG.isDebugEnabled()) {
        start = System.currentTimeMillis();
      }
      _searcher.search(luceneQuery, freqs, shards, result, count);
      if (LOG.isDebugEnabled()) {
        final long end = System.currentTimeMillis();
        LOG.debug("Search took " + (end - start) / 1000.0 + "sec.");
        completeSearchTime += (end - start);
      }
    } else {
      LOG.error("No searcher for index found on '" + getName() + "'.");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
      final DataOutputBuffer buffer = new DataOutputBuffer();
      result.write(buffer);
      LOG.debug("Result size to transfer: " + buffer.getLength());
    }
    return result;
  }

  public HitsMapWritable search(final QueryWritable query, final DocumentFrequenceWritable freqs, final String[] shards)
          throws IOException {
    return search(query, freqs, shards, Integer.MAX_VALUE - 1);
  }

  public MapWritable getDetails(final String shard, final int docId, final String[] fieldNames) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = _searcher.doc(shard, docId);
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

  @SuppressWarnings("unchecked")
  public MapWritable getDetails(final String shard, final int docId) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = _searcher.doc(shard, docId);
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

  public DocumentFrequenceWritable getDocFreqs(final QueryWritable input, final String[] shards) throws IOException {
    Query luceneQuery = input.getQuery();

    final Query rewrittenQuery = _searcher.rewrite(luceneQuery, shards);
    final DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();

    final HashSet<Term> termSet = new HashSet<Term>();
    rewrittenQuery.extractTerms(termSet);
    int numDocs = 0;
    for (final String shard : shards) {
      final java.util.Iterator<Term> termIterator = termSet.iterator();
      while (termIterator.hasNext()) {
        final Term term = termIterator.next();
        final int docFreq = _searcher.docFreq(shard, term);
        docFreqs.put(term.field(), term.text(), docFreq);
      }
      numDocs += _searcher.getNumDoc(shard);
    }
    docFreqs.setNumDocs(numDocs);
    return docFreqs;
  }

  @Override
  protected void undeploy(String shard) {
    _searcher.removeShard(shard);
  }

  @Override
  protected void setup() {
    _searcher = new KattaMultiSearcher(getName());
  }

  /*
   * Creates an index search and adds it to the KattaMultiSearch
   */
  protected void deploy(final String shardName, final File localShardFolder) throws IOException {
    IndexSearcher indexSearcher = new IndexSearcher(localShardFolder.getAbsolutePath());
    _searcher.addShard(shardName, indexSearcher);
  }

  @Override
  protected Map<String, String> getMetaData(String shardName) {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put(NUM_OF_DOCS, "" + _searcher.getNumDoc(shardName));
    return map;
  }

  @Override
  protected void tearDown() {
    // nothing to do

  }

}
