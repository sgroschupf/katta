package net.sf.katta.lib.lucene;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class WrappingGlobalDfTermsEnum extends TermsEnum {
  private final TermsEnum _termsEnum;
  private final Map<TermWritable, Integer> _dfMap;
  private final String _field;

  private Integer _currentDf;

  public WrappingGlobalDfTermsEnum(Map<TermWritable, Integer> dfMap, TermsEnum termsEnum, String field) {
    this._dfMap = dfMap;
    this._termsEnum = termsEnum;
    this._field = field;
  }

  public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
    _currentDf = _dfMap.get(new TermWritable(_field, text.utf8ToString()));
    return _termsEnum.seekCeil(text, useCache);
  }

  public void seekExact(long ord) throws IOException {
    _termsEnum.seekExact(ord);
    _currentDf = _dfMap.get(new TermWritable(_field, term().utf8ToString()));
  }

  public BytesRef term() throws IOException {
    return _termsEnum.term();
  }

  public long ord() throws IOException {
    return _termsEnum.ord();
  }

  public int docFreq() throws IOException {
    if (_currentDf != null) {
      return _currentDf;
    } else {
      return 0;
    }
  }

  public long totalTermFreq() throws IOException {
    // TODO do we need the real total term freq for anything? probably depends on similarity
    if (_currentDf != null) {
      return _currentDf;
    } else {
      return 0;
    }
  }

  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    return _termsEnum.docs(liveDocs, reuse, flags);
  }

  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags)
      throws IOException
  {
    return _termsEnum.docsAndPositions(liveDocs, reuse, flags);
  }

  public BytesRef next() throws IOException {
    return _termsEnum.next();
  }

  public Comparator<BytesRef> getComparator() {
    return _termsEnum.getComparator();
  }

  public TermState termState() throws IOException {
    TermState termState = _termsEnum.termState();
    return termState;
  }
}
