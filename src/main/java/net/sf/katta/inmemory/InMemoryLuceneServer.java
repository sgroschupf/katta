package net.sf.katta.inmemory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import net.sf.katta.node.DocumentFrequencyWritable;
import net.sf.katta.node.Hit;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.QueryWritable;

import org.apache.hadoop.io.MapWritable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;

public class InMemoryLuceneServer implements INodeManaged, IInMemoryLuceneServer {
  private String _nodeName;

  private IndexWriter _indexWriter;
  private IndexSearcher _indexSearcher;
  private RAMDirectory _d;

  public InMemoryLuceneServer(Analyzer analyzer, MaxFieldLength maxFieldLength) throws CorruptIndexException,
          LockObtainFailedException, IOException {
    _d = new RAMDirectory("/katta-inmem-test");
    _indexWriter = new IndexWriter(_d, analyzer, true, maxFieldLength);
    _indexSearcher = new IndexSearcher(_d);
  }

  @Override
  public void addShard(String shardName, File shardDir) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> getShardMetaData(String shardName) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void removeShard(String shardName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  @Override
  public void shutdown() throws Exception {
    // TODO

  }

  @SuppressWarnings("deprecation")
  @Override
  public void addDocument(Document document) throws IOException {
//    System.out.println("InMemoryLuceneServer.addDocument()");
    _indexWriter.addDocument(document);
    _indexWriter.commit();
    _indexSearcher = new IndexSearcher(_d);
  }

  @Override
  public MapWritable getDetails(int docId, String[] fields) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DocumentFrequencyWritable getDocFreqs(QueryWritable input) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, int count) throws IOException {
//    System.out.println("InMemoryLuceneServer.search()");
    TopDocs search = _indexSearcher.search(query.getQuery(), count);

    HitsMapWritable hits = new HitsMapWritable(_nodeName);
    hits.addTotalHits(search.totalHits);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc scoreDoc : scoreDocs) {

      hits.addHitToShard("", new Hit("", _nodeName, scoreDoc.score, scoreDoc.doc));
    }

    return hits;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}
