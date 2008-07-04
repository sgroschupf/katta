package net.sf.katta.index.indexer.merge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentInformation implements Writable {

  private IntWritable _docId = new IntWritable();
  private Text _sortValue = new Text();
  private Text _indexPath = new Text();

  public void write(DataOutput dataOutput) throws IOException {
    _docId.write(dataOutput);
    _sortValue.write(dataOutput);
    _indexPath.write(dataOutput);
  }

  public void readFields(DataInput dataInput) throws IOException {
    _docId.readFields(dataInput);
    _sortValue.readFields(dataInput);
    _indexPath.readFields(dataInput);
  }

  public IntWritable getDocId() {
    return _docId;
  }

  public void setDocId(int docId) {
    _docId.set(docId);
  }

  public Text getSortValue() {
    return _sortValue;
  }

  public void setSortValue(String sortValue) {
    _sortValue.set(sortValue);
  }

  public Text getIndexPath() {
    return _indexPath;
  }

  public void setIndexPath(String indexPath) {
    _indexPath.set(indexPath);
  }
}