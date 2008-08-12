package net.sf.katta.node;

import net.sf.katta.util.KattaException;
import net.sf.katta.zk.ZKClient;

public interface IAnnouncer {

  void announce(ZKClient client) throws KattaException;
}
