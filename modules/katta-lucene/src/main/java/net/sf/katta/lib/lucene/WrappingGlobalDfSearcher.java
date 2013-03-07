package net.sf.katta.lib.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WrappingGlobalDfSearcher extends IndexSearcher {
  public WrappingGlobalDfSearcher(IndexSearcher searcher, Map<TermWritable, Integer> dfMap, int numDocs) throws IOException {
    super(new WrappingGlobalDfReader(searcher.getIndexReader(), dfMap, numDocs));
  }

  public IndexReader getIndexReader() {
    return super.getIndexReader();
  }

  public Document doc(int docID) throws IOException {
    return super.doc(docID);
  }

  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    super.doc(docID, fieldVisitor);    
  }

  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return super
        .doc(docID, fieldsToLoad);    
  }

  public void setSimilarity(Similarity similarity) {
    super.setSimilarity(similarity);    
  }

  public Similarity getSimilarity() {
    return super.getSimilarity();
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, int n) throws IOException {
    return super
        .searchAfter(after, query, n);    
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n) throws IOException {
    return super.searchAfter(after, query, filter,
        n);    
  }

  public TopDocs search(Query query, int n) throws IOException {
    return super.search(query, n);    
  }

  public TopDocs search(Query query, Filter filter, int n) throws IOException {
    return super
        .search(query, filter, n);    
  }

  public void search(Query query, Filter filter, Collector results) throws IOException {
    super
        .search(query, filter, results);    
  }

  public void search(Query query, Collector results) throws IOException {
    super.search(query, results);    
  }

  public TopFieldDocs search(Query query, Filter filter, int n, Sort sort) throws IOException {
    return super
        .search(query, filter, n, sort);    
  }

  public TopFieldDocs search(Query query, Filter filter, int n, Sort sort, boolean doDocScores, boolean doMaxScore)
      throws IOException
  {
    return super.search(query, filter, n, sort, doDocScores,
        doMaxScore);    
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort) throws IOException {
    return super.searchAfter(after, query, filter, n,
        sort);    
  }

  public TopFieldDocs search(Query query, int n, Sort sort) throws IOException {
    return super.search(query, n, sort);    
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, int n, Sort sort) throws IOException {
    return super.searchAfter(after, query, n,
        sort);    
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort, boolean doDocScores,
                             boolean doMaxScore) throws IOException
  {
    return super.searchAfter(after, query, filter, n, sort, doDocScores,
        doMaxScore);    
  }

  public Query rewrite(Query original) throws IOException {
    return super.rewrite(original);    
  }

  public Explanation explain(Query query, int doc) throws IOException {
    return super.explain(query, doc);    
  }

  public Weight createNormalizedWeight(Query query) throws IOException {
    return super
        .createNormalizedWeight(query);    
  }

  public IndexReaderContext getTopReaderContext() {
    return super.getTopReaderContext();
  }

  public String toString() {
    return super.toString();    
  }

  public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
    return super
        .termStatistics(term, context);    
  }

  public CollectionStatistics collectionStatistics(String field) throws IOException {
    return super
        .collectionStatistics(field);    
  }

  private static final Method GET_SEQUENTIAL_SUB_READERS_METHOD;
  static {
    try {
      GET_SEQUENTIAL_SUB_READERS_METHOD = CompositeReader.class.getDeclaredMethod("getSequentialSubReaders");
      GET_SEQUENTIAL_SUB_READERS_METHOD.setAccessible(true);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }

  private static class WrappingGlobalDfReader extends CompositeReader {
    private BaseCompositeReader<AtomicReader> _indexReader;
    private Map<TermWritable, Integer> _dfMap;
    private int _numDocs;

    public WrappingGlobalDfReader(IndexReader indexReader, Map<TermWritable, Integer> dfMap, int numDocs) throws IOException {
      super();
      this._indexReader = (BaseCompositeReader<AtomicReader>)indexReader;
      this._dfMap = dfMap;
      this._numDocs = numDocs;
    }

    public Fields getTermVectors(int docID) throws IOException {
      return _indexReader.getTermVectors(docID);
    }

    public int numDocs() {
      return _numDocs;
    }

    public int maxDoc() {
      // TODO this is probably bad becuase I think this is supposed to be used for array sizes..
      return _numDocs;
      //return _indexReader.maxDoc();
    }

    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
      _indexReader.document(docID, visitor);
    }

    public boolean hasDeletions() {
      return _indexReader.hasDeletions();
    }

    protected void doClose() throws IOException {
      _indexReader.close();
    }

    public int docFreq(Term term) throws IOException {
      Integer val = _dfMap.get(new TermWritable(term.field(), term.text()));
      if (val != null) {
        return val;
      } else {
        return 0;
      }
    }

    public long totalTermFreq(Term term) throws IOException {
      // TODO do we need to return a real value? probably depends on the similarity used.
      return docFreq(term);
    }

    protected List<? extends IndexReader> getSequentialSubReaders() {
      try {
        List<? extends IndexReader> indexReaders = (List<? extends IndexReader>) GET_SEQUENTIAL_SUB_READERS_METHOD.invoke(_indexReader);
        List<IndexReader> wrappedIndexReaders = new ArrayList<IndexReader>();
        for (IndexReader indexReader : indexReaders) {
          wrappedIndexReaders.add(new WrappingGlobalDfAtomicReader(_dfMap, (AtomicReader) indexReader));
        }
        return wrappedIndexReaders;
      }
      catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
      catch (InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
