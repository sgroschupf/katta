package net.sf.katta.zk;

import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.Writable;

public interface IZkDataListener<W extends Writable> {

  void handleDataChange(String dataPath, W data) throws KattaException;

  void handleDataAdded(String dataPath, W data) throws KattaException;

  void handleDataDeleted(String dataPath) throws KattaException;

  W createWritable();
}
