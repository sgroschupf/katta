package net.sf.katta.loadtest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LoadTestQueryResult implements Writable {

  private long _startTime;
  private long _endTime;
  private String _query;
  private String _nodeId;

  public LoadTestQueryResult() {
    // do nothing
  }

  public LoadTestQueryResult(long startTime, long endTime, String query, String nodeId) {
    _startTime = startTime;
    _endTime = endTime;
    _query = query;
    _nodeId = nodeId;
  }

  public long getStartTime() {
    return _startTime;
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
  }

  public String getQuery() {
    return _query;
  }

  public void setQuery(String query) {
    _query = query;
  }

  public String getNodeId() {
    return _nodeId;
  }

  public void setNodeId(String nodeId) {
    _nodeId = nodeId;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    _startTime = dataInput.readLong();
    _endTime = dataInput.readLong();
    _query = dataInput.readUTF();
    _nodeId = dataInput.readUTF();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(_startTime);
    dataOutput.writeLong(_endTime);
    dataOutput.writeUTF(_query);
    dataOutput.writeUTF(_nodeId);
  }
}
