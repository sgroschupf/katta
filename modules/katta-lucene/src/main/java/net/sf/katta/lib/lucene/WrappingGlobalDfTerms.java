package net.sf.katta.lib.lucene;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class WrappingGlobalDfTerms extends Terms {
  private final Map<TermWritable, Integer> _dfMap;
  private final String _field;
  private final Terms _terms;

  public WrappingGlobalDfTerms(Map<TermWritable, Integer> dfMap, String field, Terms terms) {
    this._dfMap = dfMap;
    this._field = field;
    this._terms = terms;
  }

  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    // TODO reuse?
    return new WrappingGlobalDfTermsEnum(_dfMap, _terms.iterator(null), _field);
  }

  public Comparator<BytesRef> getComparator() throws IOException {
    return _terms.getComparator();
  }

  public long size() throws IOException {
    return _terms.size();
  }

  public long getSumTotalTermFreq() throws IOException {
    return _terms.getSumTotalTermFreq();
  }

  public long getSumDocFreq() throws IOException {
    return _terms.getSumDocFreq();
  }

  public int getDocCount() throws IOException {
    return _terms.getDocCount();
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
}
