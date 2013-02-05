package net.sf.katta.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Node selection policy that shuffles the node list when updating, thus
 * ensuring multiple clients won't connect to the nodes in the same order.
 */
public class ShuffleNodeSelectionPolicy extends BasicNodeSelectionPolicy {
  @Override
  public void update(String shard, Collection<String> nodes) {
    List<String> nodesShuffled = new ArrayList<String>(nodes);
    Collections.shuffle(nodesShuffled);
    super.update(shard, nodesShuffled);
  }
}
