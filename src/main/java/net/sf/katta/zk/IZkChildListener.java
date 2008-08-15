package net.sf.katta.zk;

import net.sf.katta.util.KattaException;

public interface IZkChildListener {

  void handleChildChange(String parentPath) throws KattaException;
}
