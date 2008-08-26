package net.sf.katta.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ShardError implements Writable {

  private String _errorMsg;
  private long _timestamp = System.currentTimeMillis();

  public ShardError() {
    // for serialization
  }

  public ShardError(String errorMsg) {
    _errorMsg = errorMsg;
  }

  public String getErrorMsg() {
    return _errorMsg;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public void readFields(final DataInput in) throws IOException {
    _timestamp = in.readLong();
    _errorMsg = in.readUTF();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeLong(_timestamp);
    out.writeUTF(_errorMsg);
  }
}
