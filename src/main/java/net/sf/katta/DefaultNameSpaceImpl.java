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
package net.sf.katta;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

/**
 * Implements the default name space in zookeeper for this katta instance.
 */
public class DefaultNameSpaceImpl implements IDefaultNameSpace {

  private static final Logger LOG = Logger.getLogger(DefaultNameSpaceImpl.class);

  private ZkConfiguration _conf;

  public DefaultNameSpaceImpl(ZkConfiguration conf) {
    _conf = conf;
  }

  @Override
  public void createDefaultNameSpace(ZkClient zkClient) {
    LOG.debug("Creating default File structure if required....");
    safeCreate(zkClient, _conf.getZkRootPath());
    PathDef[] values = PathDef.values();
    for (PathDef pathDef : values) {
      if (pathDef != PathDef.MASTER && pathDef != PathDef.VERSION) {
        safeCreate(zkClient, _conf.getZkPath(pathDef));
      }
    }
  }

  private void safeCreate(ZkClient zkClient, String path) {
    try {
      // first create parent directories
      String parent =ZkConfiguration.getZkParent(path);
      if (parent != null && !zkClient.exists(parent)) {
        safeCreate(zkClient, parent);
      }

      zkClient.createPersistent(path);
    } catch (ZkNodeExistsException e) {
      // Ignore if the node already exists.
    }
  }
}
