package net.sf.katta.lib.lucene;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class GlobalDfTerms extends Terms {
  private final Map<TermWritable, Integer> _dfMap;
  private final String _field;

  public GlobalDfTerms(String field, Map<TermWritable,Integer> dfMap) {
    this._field = field;
    this._dfMap = dfMap;
  }

  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    return new GlobalDfTermsEnum(_field, _dfMap);
  }

  public Comparator<BytesRef> getComparator() throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public long size() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public long getSumTotalTermFreq() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public long getSumDocFreq() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public int getDocCount() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public boolean hasOffsets() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public boolean hasPositions() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public boolean hasPayloads() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
