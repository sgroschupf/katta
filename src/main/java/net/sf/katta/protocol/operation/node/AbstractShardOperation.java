package net.sf.katta.protocol.operation.node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.katta.node.NodeContext;
import net.sf.katta.util.KattaException;

import org.apache.log4j.Logger;

public abstract class AbstractShardOperation implements NodeOperation {

  private static final long serialVersionUID = 1L;
  protected final static Logger LOG = Logger.getLogger(AbstractShardOperation.class);

  private Map<String, String> _shardPathesByShardNames = new HashMap<String, String>(3);

  public Set<String> getShardNames() {
    return _shardPathesByShardNames.keySet();
  }

  public String getShardPath(String shardName) {
    return _shardPathesByShardNames.get(shardName);
  }

  public void addShard(String shardName, String shardPath) {
    _shardPathesByShardNames.put(shardName, shardPath);
  }

  public void addShard(String shardName) {
    _shardPathesByShardNames.put(shardName, null);
  }

  @Override
  public final void execute(NodeContext context) {
    LOG.info(getOperationName() + " shards: " + getShardNames());
    for (String shardName : getShardNames()) {
      try {
        LOG.info(getOperationName() + " shard '" + shardName + "'");
        execute(context, shardName);
      } catch (Exception e) {
        LOG.error("failed to " + getOperationName() + " shard '" + shardName + "' on node '"
                + context.getNode().getName() + "'", e);
        onException(context, shardName, e);
      }
    }
  }

  protected abstract String getOperationName();

  protected abstract void execute(NodeContext context, String shardName) throws Exception;

  protected abstract void onException(NodeContext context, String shardName, Exception e);

  /*
   * Announce in zookeeper node is serving this shard,
   */
  protected void announceShard(String shardName, NodeContext context) throws KattaException {
    LOG.info("announce shard '" + shardName + "'");
    Map<String, String> metaData;
    try {
      metaData = context.getNodeManaged().getShardMetaData(shardName);
    } catch (Throwable t) {
      throw new KattaException("Error retrieving shard metadata for " + shardName, t);
    }
    context.getProtocol().publishShard(context.getNode(), shardName, metaData);
  }

}
