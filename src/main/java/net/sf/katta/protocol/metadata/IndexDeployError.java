package net.sf.katta.protocol.metadata;

import java.io.Serializable;
import java.util.List;

import net.sf.katta.util.One2ManyListMap;

public class IndexDeployError implements Serializable {

  private static final long serialVersionUID = 1L;

  public static enum ErrorType {
    NO_NODES_AVAILIBLE, INDEX_NOT_ACCESSIBLE, SHARDS_NOT_DEPLOYABLE, UNKNOWN;
  }

  private final String _indexName;
  private final ErrorType _errorType;
  private final One2ManyListMap<String, Exception> _shard2ExceptionsMap = new One2ManyListMap<String, Exception>();
  private Exception _exception;

  public IndexDeployError(String indexName, ErrorType errorType) {
    _indexName = indexName;
    _errorType = errorType;
  }

  public String getIndexName() {
    return _indexName;
  }

  public ErrorType getErrorType() {
    return _errorType;
  }

  public void setException(Exception exception) {
    _exception = exception;
  }

  public Exception getException() {
    return _exception;
  }

  public void addShardError(String shardName, Exception exception) {
    _shard2ExceptionsMap.add(shardName, exception);
  }

  public List<Exception> getShardErrors(String shardName) {
    return _shard2ExceptionsMap.getValues(shardName);
  }
}
