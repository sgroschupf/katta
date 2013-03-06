package net.sf.katta.lib.lucene;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class GlobalDfFields extends Fields {
  private final Map<TermWritable, Integer> _dfMap;

  public GlobalDfFields(Map<TermWritable, Integer> dfMap) {
    this._dfMap = dfMap;
  }

  public Iterator<String> iterator() {
    throw new UnsupportedOperationException();
  }

  public Terms terms(String field) throws IOException {
    return new GlobalDfTerms(field, _dfMap);
  }

  public int size() {
    throw new UnsupportedOperationException();
  }
}
