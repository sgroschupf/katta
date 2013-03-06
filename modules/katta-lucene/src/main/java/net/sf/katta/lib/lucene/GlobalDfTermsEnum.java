package net.sf.katta.lib.lucene;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class GlobalDfTermsEnum extends TermsEnum {
  private final String _field;
  private final Map<TermWritable, Integer> _dfMap;

  private Integer lastDf = null;
  private BytesRef lastTerm = null;

  public GlobalDfTermsEnum(String field, Map<TermWritable, Integer> dfMap) {
    this._field = field;
    this._dfMap = dfMap;
  }

  public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
    TermWritable termWritable = new TermWritable(_field, text.toString());
    lastDf = _dfMap.get(termWritable);
    lastTerm = text;
    if (lastDf != null) {
      return SeekStatus.FOUND;
    } else {
      return SeekStatus.NOT_FOUND;
    }
  }

  public void seekExact(long ord) throws IOException {
    throw new UnsupportedOperationException();
  }

  public BytesRef term() throws IOException {
    return lastTerm;
  }

  public long ord() throws IOException {
    throw new UnsupportedOperationException();
  }

  public int docFreq() throws IOException {
    if (lastDf != null) {
      return lastDf;
    } else {
      return -1;
    }
  }

  public long totalTermFreq() throws IOException {
    return -1;
  }

  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    throw new UnsupportedOperationException();
  }

  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags)
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

  public BytesRef next() throws IOException {
    throw new UnsupportedOperationException();
  }

  public Comparator<BytesRef> getComparator() {
    throw new UnsupportedOperationException();
  }
}
