package net.sf.katta.inmemory;

import junit.framework.TestCase;
import net.sf.katta.node.DocumentFrequencyWritable;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.QueryWritable;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.TermQuery;

public class InMemoryLuceneServerTest extends TestCase {

  public void testAddDocs() throws Exception {
    InMemoryLuceneServer server = new InMemoryLuceneServer(new SimpleAnalyzer(), new MaxFieldLength(1000));
    long start = System.currentTimeMillis();
    String key = "test";

    int MAXIT = 1000;
    
    for (int i = 0; i < MAXIT; i++) {
      Document document = new Document();
      String value = "testValue" + i;
      document.add(new Field(key, value, Store.YES, Index.NOT_ANALYZED));
      server.addDocument(document);
    }
    System.out.println("add took:" + (System.currentTimeMillis() - start));

    long searchStart = System.currentTimeMillis();
    for (int i = 0; i < MAXIT; i++) {
      String value = "testValue" + i;
      TermQuery query = new TermQuery(new Term(key, value));

      DocumentFrequencyWritable freq = new DocumentFrequencyWritable();
      HitsMapWritable search = server.search(new QueryWritable(query), freq, 10);
      assertEquals(1, search.getTotalHits());
    }
    System.out.println("search took:" + (System.currentTimeMillis() - searchStart));
    
    System.out.println("all took:" + (System.currentTimeMillis() - start));
  }
}
