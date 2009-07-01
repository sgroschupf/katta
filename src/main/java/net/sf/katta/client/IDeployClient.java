/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.client;

import java.util.List;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.KattaException;

public interface IDeployClient {

  IIndexDeployFuture addIndex(final String name, final String path,
      final int replicationLevel) throws KattaException;

  void removeIndex(final String name) throws KattaException;

  boolean existsIndex(String name) throws KattaException;

  List<IndexMetaData> getIndexes(IndexState indexState) throws KattaException;

  List<String> getIndexNames(IndexState indexState) throws KattaException;

  IndexMetaData getIndexMetaData(String name) throws KattaException;

  void disconnect();

}
