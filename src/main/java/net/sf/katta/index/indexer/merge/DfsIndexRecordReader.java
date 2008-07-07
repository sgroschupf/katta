package net.sf.katta.index.indexer.merge;

import java.io.FileNotFoundException;
import java.io.IOException;

import net.sf.katta.util.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;


public class DfsIndexRecordReader implements RecordReader<Text, DocumentInformation> {

  private IDocumentDuplicateInformation _duplicateInformation;

  private IndexReader _indexReader;

  private int _maxDoc;

  private int _doc;

  private Path _indexPath;

  public static final String INVALID = "INVALID";


  public DfsIndexRecordReader(JobConf jobConf, InputSplit inputSplit, IDocumentDuplicateInformation duplicateInformation) throws IOException {
    _duplicateInformation = duplicateInformation;
    FileSystem fileSystem = FileSystem.get(jobConf);
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path indexPath = fileSplit.getPath();
    //we use md5 for uncompressed folder, because some shards can have the same name
    String md5 = MD5Hash.digest(indexPath.toString()).toString();
    Path workingFolder = new Path(jobConf.getOutputPath(), ".indexes/" + indexPath.getName() + "-" + md5 + "-uncompress");
    //the outputpath is modified by hadoop and will be extend with "_temporary/jobId"
    _indexPath = new Path(jobConf.getOutputPath().getParent().getParent(), ".indexes/" + indexPath.getName() + "-" + md5 + "-uncompress");
    try {
      _indexReader = IndexReader.open(new DfsIndexDirectory(fileSystem, indexPath, workingFolder));
      _maxDoc = _indexReader.maxDoc();
    } catch (FileNotFoundException e) {
      Logger.warn("can not open index '" + indexPath + "', ignore this index.", e);
    }
  }

  public boolean next(Text key, DocumentInformation value) throws IOException {
    boolean ret = false;
    if (_doc < _maxDoc) {
      ret = true;
      Document document = _indexReader.document(_doc);
      String keyInfo = _duplicateInformation.getKey(document);
      String sortValue = _duplicateInformation.getSortValue(document);

      if ((keyInfo == null || keyInfo.trim().equals(""))) {
        Logger.warn("key can not be extracted from the lucene document, this document will not be collected.");
        keyInfo = INVALID;
      }

      if ((sortValue == null || sortValue.trim().equals(""))) {
        Logger.warn("sortValue can not be extracted from the lucene document, this document will be collect with the lowest sort value.");
        sortValue = "" + Integer.MIN_VALUE;
      }

      key.set(keyInfo);
      value.setDocId(_doc);
      value.setSortValue(sortValue);
      value.setIndexPath(_indexPath.toString());
      _doc++;
    }
    return ret;
  }

  public Text createKey() {
    return new Text();
  }

  public DocumentInformation createValue() {
    return new DocumentInformation();
  }

  public long getPos() throws IOException {
    return _maxDoc > 0 ? _doc / _maxDoc : 1;
  }

  public void close() throws IOException {
    if (_indexReader != null) {
      _indexReader.close();
    }
  }

  public float getProgress() throws IOException {
    return 0;
  }
}
