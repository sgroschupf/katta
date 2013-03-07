package net.sf.katta.lib.lucene;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class WrappingGlobalDfFields extends Fields {
  private final Map<TermWritable, Integer> _dfMap;
  private final Fields _fields;

  public WrappingGlobalDfFields(Map<TermWritable, Integer> dfMap, Fields fields) {
    this._dfMap = dfMap;
    this._fields = fields;
  }

  public Iterator<String> iterator() {
    return _fields.iterator();
  }

  public Terms terms(String field) throws IOException {
    Terms terms = _fields.terms(field);
    if (terms != null) {
      return new WrappingGlobalDfTerms(_dfMap, field, terms);
    } else {
      return null;
    }
  }

  public int size() {
    return _fields.size();
  }
}
