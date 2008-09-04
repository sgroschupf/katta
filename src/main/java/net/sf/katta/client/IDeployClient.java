package net.sf.katta.client;

import java.util.List;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.KattaException;

public interface IDeployClient {

  IIndexDeployFuture addIndex(final String name, final String path, final String analyzerClass,
      final int replicationLevel) throws KattaException;

  void removeIndex(final String name) throws KattaException;

  boolean existsIndex(String indexName) throws KattaException;

  List<IndexMetaData> getIndexes(IndexState indexState) throws KattaException;

  List<String> getIndexNames(IndexState indexState) throws KattaException;

  void disconnect();

}
