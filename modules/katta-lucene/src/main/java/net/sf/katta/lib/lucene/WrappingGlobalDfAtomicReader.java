package net.sf.katta.lib.lucene;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

public class WrappingGlobalDfAtomicReader extends AtomicReader {
  private final Map<TermWritable, Integer> _dfMap;
  private final AtomicReader _reader;

  public WrappingGlobalDfAtomicReader(Map<TermWritable, Integer> dfMap, AtomicReader reader) {
    this._dfMap = dfMap;
    this._reader = reader;
  }

  public Fields fields() throws IOException {
    return new WrappingGlobalDfFields(_dfMap, _reader.fields());
  }

  public DocValues docValues(String field) throws IOException {
    return _reader.docValues(field);
  }

  public DocValues normValues(String field) throws IOException {
    return _reader.normValues(field);
  }

  public FieldInfos getFieldInfos() {
    return _reader.getFieldInfos();
  }

  public Bits getLiveDocs() {
    return _reader.getLiveDocs();
  }

  public Fields getTermVectors(int docID) throws IOException {
    return _reader.getTermVectors(docID);
  }

  public int numDocs() {
    // TODO should this be the global numDocs?
    return _reader.numDocs();
  }

  public int maxDoc() {
    // TODO should this be the global numDocs?
    return _reader.maxDoc();
  }

  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    document(docID, visitor);
  }

  public boolean hasDeletions() {
    return _reader.hasDeletions();
  }

  protected void doClose() throws IOException {
    _reader.close();
  }
}
