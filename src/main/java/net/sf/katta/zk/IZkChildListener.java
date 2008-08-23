package net.sf.katta.zk;

import java.util.List;

import net.sf.katta.util.KattaException;

/**
 * An {@link IZkChildListener} can be registered at a {@link ZKClient} for
 * listening on zk child changes for a given path.
 * 
 * Node: Also this listener re-subscribes it watch for the path on each zk event
 * (zk watches are one-timers) is is not guaranteed that events on the path are
 * missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
 * implementation of this class should take that into account.
 * 
 */
public interface IZkChildListener {

  void handleChildChange(String parentPath, List<String> currentChilds) throws KattaException;
}
