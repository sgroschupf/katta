package net.sf.katta.zk;

import net.sf.katta.util.KattaException;

public interface IZkDataListener {

  void handleDataChange(String parentPath) throws KattaException;
}
