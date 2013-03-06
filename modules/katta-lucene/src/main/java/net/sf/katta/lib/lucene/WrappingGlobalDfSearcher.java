package net.sf.katta.lib.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class WrappingGlobalDfSearcher extends IndexSearcher {
  public WrappingGlobalDfSearcher(IndexSearcher searcher, Map<TermWritable, Integer> dfMap) throws IOException {
    super(new WrappingGlobalDfReader(searcher.getIndexReader(), dfMap));
  }

  public WrappingGlobalDfSearcher(IndexReader r) {
    super(r);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public WrappingGlobalDfSearcher(IndexReader r, ExecutorService executor) {
    super(r, executor);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public WrappingGlobalDfSearcher(IndexReaderContext context, ExecutorService executor) {
    super(context, executor);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public WrappingGlobalDfSearcher(IndexReaderContext context) {
    super(context);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public IndexReader getIndexReader() {
    return super.getIndexReader();    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Document doc(int docID) throws IOException {
    return super.doc(docID);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    super.doc(docID, fieldVisitor);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return super
        .doc(docID, fieldsToLoad);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public void setSimilarity(Similarity similarity) {
    super.setSimilarity(similarity);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Similarity getSimilarity() {
    return super.getSimilarity();    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, int n) throws IOException {
    return super
        .searchAfter(after, query, n);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n) throws IOException {
    return super.searchAfter(after, query, filter,
        n);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs search(Query query, int n) throws IOException {
    return super.search(query, n);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs search(Query query, Filter filter, int n) throws IOException {
    return super
        .search(query, filter, n);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public void search(Query query, Filter filter, Collector results) throws IOException {
    super
        .search(query, filter, results);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public void search(Query query, Collector results) throws IOException {
    super.search(query, results);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopFieldDocs search(Query query, Filter filter, int n, Sort sort) throws IOException {
    return super
        .search(query, filter, n, sort);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopFieldDocs search(Query query, Filter filter, int n, Sort sort, boolean doDocScores, boolean doMaxScore)
      throws IOException
  {
    return super.search(query, filter, n, sort, doDocScores,
        doMaxScore);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort) throws IOException {
    return super.searchAfter(after, query, filter, n,
        sort);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopFieldDocs search(Query query, int n, Sort sort) throws IOException {
    return super.search(query, n, sort);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, int n, Sort sort) throws IOException {
    return super.searchAfter(after, query, n,
        sort);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort, boolean doDocScores,
                             boolean doMaxScore) throws IOException
  {
    return super.searchAfter(after, query, filter, n, sort, doDocScores,
        doMaxScore);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Query rewrite(Query original) throws IOException {
    return super.rewrite(original);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Explanation explain(Query query, int doc) throws IOException {
    return super.explain(query, doc);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public Weight createNormalizedWeight(Query query) throws IOException {
    return super
        .createNormalizedWeight(query);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public IndexReaderContext getTopReaderContext() {
    return super.getTopReaderContext();    //To change body of overridden methods use File | Settings | File Templates.
  }

  public String toString() {
    return super.toString();    //To change body of overridden methods use File | Settings | File Templates.
  }

  public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
    return super
        .termStatistics(term, context);    //To change body of overridden methods use File | Settings | File Templates.
  }

  public CollectionStatistics collectionStatistics(String field) throws IOException {
    return super
        .collectionStatistics(field);    //To change body of overridden methods use File | Settings | File Templates.
  }

  private static class WrappingGlobalDfReader extends AtomicReader {
    private SlowCompositeReaderWrapper _indexReader;
    private Map<TermWritable, Integer> _dfMap;

    public WrappingGlobalDfReader(IndexReader indexReader, Map<TermWritable, Integer> dfMap) throws IOException {
      super();
      this._indexReader = new SlowCompositeReaderWrapper((CompositeReader)indexReader);
      this._dfMap = dfMap;
    }

    public Fields fields() throws IOException {
      return new Fields() {
        private Fields _fields = _indexReader.fields();
        public Iterator<String> iterator() {
          return _fields.iterator();
        }

        public Terms terms(final String field) throws IOException {
          return new Terms() {
            private Terms _terms = _fields.terms(field);
            // TODO support reuse?
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
              return new TermsEnum() {
                private TermsEnum _termsEnum = _terms != null ? _terms.iterator(null) : null;
                private Integer currentDfVal;
                private BytesRef _term;

                public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
                  // TODO this may be non-optimal having to toString!
                  currentDfVal = _dfMap.get(new TermWritable(field, text.toString()));
                  SeekStatus wrappedSeekStatus = SeekStatus.NOT_FOUND;
                  if (_termsEnum != null) {
                    wrappedSeekStatus = _termsEnum.seekCeil(text, useCache);
                  }
                  if (currentDfVal != null) {
                    _term = text;
                    return SeekStatus.FOUND;
                  } else {
                    return wrappedSeekStatus;
                  }
                }

                public void seekExact(long ord) throws IOException {
                  if (_termsEnum != null) {
                    _termsEnum.seekExact(ord);
                  } else {
                    throw new UnsupportedOperationException();
                  }
                }

                public BytesRef term() throws IOException {
                  if (_termsEnum != null) {
                    return _termsEnum.term();
                  } else {
                    throw new UnsupportedOperationException();
                  }
                }

                public long ord() throws IOException {
                  if (_termsEnum != null) {
                    return _termsEnum.ord();
                  } else {
                    throw new UnsupportedOperationException();
                  }
                }

                public int docFreq() throws IOException {
                  if (currentDfVal == null) {
                    return 0;
                  }
                  return currentDfVal;
                }

                public long totalTermFreq() throws IOException {
                  //throw new UnsupportedOperationException();
                  /* TODO is it OK to use docFreq here?
                  /* if not, we need to collect doc freq and total term freq
                  */
                  return docFreq();
                  /*if (_termsEnum != null) {
                    return _termsEnum.totalTermFreq();
                  } else {
                    return 0;
                  }*/
                }

                public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
                  throw new UnsupportedOperationException();
                  //return _termsEnum.docs(liveDocs, reuse, flags);
                }

                public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags)
                    throws IOException
                {
                  throw new UnsupportedOperationException();
                  //return _termsEnum.docsAndPositions(liveDocs, reuse, flags);
                }

                public BytesRef next() throws IOException {
                  // TODO should we update the DF here too?
                  throw new UnsupportedOperationException();
                  //return _termsEnum.next();
                }

                public Comparator<BytesRef> getComparator() {
                  throw new UnsupportedOperationException();
                  //return _termsEnum.getComparator();
                }
              };
            }

            public Comparator<BytesRef> getComparator() throws IOException {
              return _terms.getComparator();
            }

            public long size() throws IOException {
              return _terms.size();
            }

            public long getSumTotalTermFreq() throws IOException {
              if (_terms != null) {
                return _terms.getSumTotalTermFreq();
              } else {
                return 0;
              }
            }

            public long getSumDocFreq() throws IOException {
              // TODO this should probably be calculated! right?
              if (_terms == null) {
                return 0;
              } else {
                return _terms.getSumDocFreq();
              }
            }

            public int getDocCount() throws IOException {
              if (_terms == null) {
                return 0;
              } else {
                return _terms.getDocCount();
              }
            }

            public boolean hasOffsets() {
              return _terms.hasOffsets();
            }

            public boolean hasPositions() {
              return _terms.hasPositions();
            }

            public boolean hasPayloads() {
              return _terms.hasPayloads();
            }
          };
        }

        public int size() {
          return _fields.size();
        }
      };
    }

    public DocValues docValues(String field) throws IOException {
      return _indexReader.docValues(field);
    }

    public DocValues normValues(String field) throws IOException {
      return _indexReader.normValues(field);
    }

    public FieldInfos getFieldInfos() {
      return _indexReader.getFieldInfos();
    }

    public Bits getLiveDocs() {
      return _indexReader.getLiveDocs();
    }

    public Fields getTermVectors(int docID) throws IOException {
      return _indexReader.getTermVectors(docID);
    }

    public int numDocs() {
      return _indexReader.numDocs();
    }

    public int maxDoc() {
      return _indexReader.maxDoc();
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
  }
}
