package net.sf.katta.inmemory;

import net.sf.katta.client.Client;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.node.Hits;
import net.sf.katta.util.KattaException;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

public class InMemoryLuceneClient extends LuceneClient {


  public InMemoryLuceneClient(ZKClient zkClient) throws KattaException {
  }



  public void addDocument(Document document) {

  }

}
