package net.sf.katta.lib.lucene;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

public class GlobalDfReader extends AtomicReader {
  private final Map<TermWritable, Integer> _dfMap;
  private final int _maxDoc;

  public GlobalDfReader(Map<TermWritable, Integer> dfMap, int maxDoc) {
    this._dfMap = dfMap;
    this._maxDoc = maxDoc;
  }

  public Fields fields() throws IOException {
    return new GlobalDfFields(_dfMap);
  }

  public DocValues docValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  public DocValues normValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  public FieldInfos getFieldInfos() {
    throw new UnsupportedOperationException();
  }

  public Bits getLiveDocs() {
    throw new UnsupportedOperationException();
  }

  public Fields getTermVectors(int docID) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int numDocs() {
    // TODO is maxDoc for this okay?
    return _maxDoc;
  }

  public int maxDoc() {
    // TODO does +1 need to be added to this?
    return _maxDoc;
  }

  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    throw new UnsupportedOperationException();
  }

  public boolean hasDeletions() {
    throw new UnsupportedOperationException();
  }

  protected void doClose() throws IOException {
    // no-op
  }
}
