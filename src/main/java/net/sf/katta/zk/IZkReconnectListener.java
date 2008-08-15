package net.sf.katta.zk;

import net.sf.katta.util.KattaException;

public interface IZkReconnectListener {

  void handleReconnect() throws KattaException;
}
