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
