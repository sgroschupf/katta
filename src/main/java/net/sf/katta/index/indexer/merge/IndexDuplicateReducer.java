package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IndexDuplicateReducer implements Reducer<Text, DocumentInformation, Text, DocumentInformation> {

  public void reduce(Text text, Iterator<DocumentInformation> iterator, OutputCollector<Text, DocumentInformation> outputCollector, Reporter reporter) throws IOException {

    //we do not collect documents whith invalid document identifier
    if (text.toString().equals(DfsIndexRecordReader.INVALID)) {
      //if we skip a lot of documents, we have to call setStatus to avoid the aborting of this job 
      reporter.setStatus("invalid document: " + text);
      return;
    }

    DocumentInformation newestInformation = null;
    Text sortValue = new Text("" + Integer.MIN_VALUE);
    while (iterator.hasNext()) {
      DocumentInformation documentInformation = iterator.next();
      Text tmpSortValue = documentInformation.getSortValue();
      int i = tmpSortValue.compareTo(sortValue);
      if (i > 0) {
        sortValue = tmpSortValue;
        newestInformation = documentInformation;
      }
    }
    outputCollector.collect(text, newestInformation);
  }

  public void configure(JobConf jobConf) {
    //nothing todo
  }

  public void close() throws IOException {
    //nothing todo
  }


}
