package net.sf.katta.index.indexer.merge;

import org.apache.lucene.document.Document;

public interface IDocumentDuplicateInformation {

  String getKey(Document document);

  String getSortValue(Document document);

}
