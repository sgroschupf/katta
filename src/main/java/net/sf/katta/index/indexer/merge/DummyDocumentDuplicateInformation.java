package net.sf.katta.index.indexer.merge;

import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

class DummyDocumentDuplicateInformation implements IDocumentDuplicateInformation {
  public String getKey(Document document) {
    List list = document.getFields();
    return list.isEmpty() ? "foo" : ((Field) list.get(0)).stringValue();
  }

  public String getSortValue(Document document) {
    return getKey(document);
  }
}
