package net.sf.katta.protocol.operation.leader;

import java.util.HashSet;
import java.util.Set;

import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;

import org.apache.log4j.Logger;

public abstract class AbstractInbalanceOperation extends AbstractDistributeShardsOperation {

  private static final long serialVersionUID = 1L;
  private final static Logger LOG = Logger.getLogger(AbstractInbalanceOperation.class);

  protected Set<String> getUnderreplicatedIndexes(final InteractionProtocol protocol, int nodeCount) {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : protocol.getIndices()) {
      final IndexMetaData indexMetaData = protocol.getIndexMD(index);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        int desiredReplicationCount = indexMetaData.getReplicationLevel();
        int minimalReplicationCount = getMinimalReplicationCount(protocol, indexMetaData);
        if (minimalReplicationCount < desiredReplicationCount) {
          if (minimalReplicationCount >= nodeCount) {
            LOG.warn("found index '" + index
                    + "' underreplicated but skip replicating since not enough nodes connected");
          } else {
            underreplicatedIndexes.add(index);
          }
        }
      }
    }
    return underreplicatedIndexes;
  }

  protected Set<String> getOverreplicatedIndexes(final InteractionProtocol protocol) {
    final Set<String> overreplicatedIndexes = new HashSet<String>();
    for (final String index : protocol.getIndices()) {
      final IndexMetaData indexMetaData = protocol.getIndexMD(index);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        if (isOverReplicated(protocol, indexMetaData)) {
          overreplicatedIndexes.add(index);
        }
      }
    }
    return overreplicatedIndexes;
  }

  private int getMinimalReplicationCount(final InteractionProtocol protocol, final IndexMetaData indexMetaData) {
    int minimalReplicationCount = indexMetaData.getReplicationLevel();
    final Set<Shard> shards = indexMetaData.getShards();
    for (final Shard shard : shards) {
      final int servingNodes = protocol.getShardNodes(shard.getName()).size();
      if (servingNodes < minimalReplicationCount) {
        minimalReplicationCount = servingNodes;
      }
    }
    return minimalReplicationCount;
  }

  private boolean isOverReplicated(final InteractionProtocol protocol, final IndexMetaData indexMetaData) {
    final Set<Shard> shards = indexMetaData.getShards();
    for (final Shard shard : shards) {
      final int servingNodes = protocol.getShardNodes(shard.getName()).size();
      if (servingNodes > indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
  }

}
