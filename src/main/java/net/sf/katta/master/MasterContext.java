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
package net.sf.katta.master;

import java.io.IOException;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.util.HadoopUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MasterContext {

  private final Master _master;
  private final InteractionProtocol _protocol;
  private final IDeployPolicy _deployPolicy;
  private final MasterQueue _masterQueue;

  public MasterContext(InteractionProtocol protocol, Master master, IDeployPolicy deployPolicy, MasterQueue masterQueue) {
    _protocol = protocol;
    _master = master;
    _deployPolicy = deployPolicy;
    _masterQueue = masterQueue;
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  public Master getMaster() {
    return _master;
  }

  public IDeployPolicy getDeployPolicy() {
    return _deployPolicy;
  }

  public MasterQueue getMasterQueue() {
    return _masterQueue;
  }

  public FileSystem getFileSystem(IndexMetaData indexMd) throws IOException {
    return HadoopUtil.getFileSystem(new Path(indexMd.getPath()));
  }

}
