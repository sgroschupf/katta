package net.sf.katta.tool;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates the test indices [abc]Index in modules/katta-lucene/src/main/resources/testIndexA
 */
public class CreateTestIndex {
  public static void main(String[] args) throws IOException {
    File file = new File(args[0]);
    createTestIndex(new File(file, "aIndex"), getIndexADocuments());
    createTestIndex(new File(file, "bIndex"), getIndexBDocuments());
    createTestIndex(new File(file, "cIndex"), getIndexCDocuments());
  }

  public static List<Document> getIndexADocuments() {
    List<Document> documents = new ArrayList<Document>(2);

    Document doc = new Document();
    doc.add(new TextField("foo", "bar", Field.Store.YES));
    documents.add(doc);

    doc = new Document();
    doc.add(new TextField("foo", "bar bar bar", Field.Store.YES));
    documents.add(doc);

    return documents;
  }

  public static List<Document> getIndexBDocuments() {
    List<Document> documents = new ArrayList<Document>(1);

    Document doc = new Document();
    doc.add(new TextField("foo", "bar", Field.Store.YES));
    doc.add(new TextField("foo", "bar bar", Field.Store.YES));
    documents.add(doc);

    return documents;
  }

  public static List<Document> getIndexCDocuments() {
    List<Document> documents = new ArrayList<Document>(1);

    Document doc = new Document();
    doc.add(new TextField("foo", "bar", Field.Store.YES));
    documents.add(doc);

    return documents;
  }

  public static void createTestIndex(File path, List<Document> documents) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_41, new StandardAnalyzer(Version.LUCENE_41));
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter indexWriter = new IndexWriter(FSDirectory.open(path), config);

    for (Document document : documents) {
      indexWriter.addDocument(document);
    }

    indexWriter.forceMerge(1);
    indexWriter.close();
  }
}
