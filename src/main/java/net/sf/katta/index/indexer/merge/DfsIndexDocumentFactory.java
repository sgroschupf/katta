package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.katta.index.indexer.IDocumentFactory;
import net.sf.katta.util.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

public class DfsIndexDocumentFactory implements IDocumentFactory<Text, DocumentInformation> {


  private FileSystem _fileSystem;

  private Map<Text, IndexReader> _readerMap = new HashMap<Text, IndexReader>();

  public Document convert(Text key, DocumentInformation value) {

    Document document = null;
    IntWritable docId = value.getDocId();
    Text text = value.getIndexPath();
    try {

      if (!_readerMap.containsKey(text)) {
        IndexReader reader = IndexReader.open(new DfsIndexDirectory(_fileSystem, null, new Path(text.toString())));
        _readerMap.put(text, reader);
      }
      IndexReader reader = _readerMap.get(text);
      document = reader.document(docId.get());
    } catch (IOException e) {
      Logger.warn("can not read document '" + docId + "'from index '" + text + "'", e);
    }
    return document;
  }

  public Analyzer getIndexAnalyzer() {
    return new StandardAnalyzer();
  }

  public void configure(JobConf jobConf) throws IOException {
    _fileSystem = FileSystem.get(jobConf);
  }
}
