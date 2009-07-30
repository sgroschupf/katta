package net.sf.katta.inmemory;

import java.io.File;

import junit.framework.TestCase;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Node;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.mockito.Mockito;

public class InMemoryLuceneTest extends TestCase {

  public void testAddDocument() throws Exception {

//    ZKClient zkClient = Mockito.mock(ZKClient.class);
//    ZKClient.ZkLock lock = new ZKClient.ZkLock();
//    Mockito.when(zkClient.getEventLock()).thenReturn(lock);
//    Mockito.when(zkClient.getConfig()).thenReturn(new ZkConfiguration());
//    
//    
//    NodeConfiguration configuration = new NodeConfiguration();
//    configuration.setStartPort(4711);
//    configuration.setShardFolder(new File("./build/data/InMemoryLuceneTest").getAbsolutePath());
//
//    
//    Node node = new Node(zkClient, configuration, new InMemoryLuceneServer());
//    node.start();
//
//    
//    
//    InMemoryLuceneClient client = new InMemoryLuceneClient(zkClient);
//    for (int i = 0; i < 10; i++) {
//      Document document = new Document();
//      String value = "testValue" + i;
//      document.add(new Field("test", value, Store.YES, Field.Index.ANALYZED));
//      client.addDocument(document);
//
//      TermQuery query = new TermQuery(new Term(value));
//      Hits search = client.search(query, 10);
//      assertEquals(1, search.size());
//
//    }
//
//    node.shutdown();
  }

}
