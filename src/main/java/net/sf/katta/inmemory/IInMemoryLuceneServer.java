package net.sf.katta.inmemory;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;

import net.sf.katta.node.DocumentFrequencyWritable;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.QueryWritable;

public interface IInMemoryLuceneServer extends VersionedProtocol {

  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, int count)
          throws IOException;

  public DocumentFrequencyWritable getDocFreqs(QueryWritable input) throws IOException;

  public MapWritable getDetails(int docId, String[] fields) throws IOException;

  public void addDocument(Document document) throws CorruptIndexException, IOException;
}
