/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.node;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Maintains the current set of shards, as specified by the Node class.
 */
public abstract class AbstractServer implements INodeManaged {

  private final static Logger LOG = Logger.getLogger(AbstractServer.class);

  protected final Map<String, File> _shards = new ConcurrentHashMap<String, File>();
  protected String _nodeName;

  public long getProtocolVersion(@SuppressWarnings("unused") final String protocol, @SuppressWarnings("unused") final long clientVersion) {
    return 0L;
  }

  public void setNodeName(String nodeName) {
    _nodeName = nodeName;
  }

  
  /**
   * Add a shard.
   * 
   * @param shardName
   * @param indexSearcher
   * @throws IOException
   */
  public void addShard(final String shardName, final File shardDir) throws IOException {
    LOG.info(_nodeName + " got shard " + shardName);
    if (!shardDir.exists()) {
      throw new IOException("Shard " + shardName + " dir " + shardDir.getAbsolutePath() + " does not exist!");
    }
    if (!shardDir.canRead()) {
      throw new IOException("Can not read shard " + shardName + " dir " + shardDir.getAbsolutePath() + "!");
    }
    _shards.put(shardName, shardDir);
  }

  /**
   * Remove a shard.
   */
  public void removeShard(final String shardName) throws IOException {
    LOG.info(_nodeName + " removing shard " + shardName);
    _shards.remove(shardName);
  }
  
  /**
   * Returns data about a shard. Currently the only standard key is
   * SHARD_SIZE_KEY. This value will be reported by the listIndexes command.
   * The units depend on the type of server. It is OK to return an empty
   * map or null.
   * 
   * @param shardName The name of the shard to measure. 
   * This was the name provided in addShard().
   * @return a map of key/value pairs which describe the shard.
   * @throws Exception 
   */
  public abstract Map<String, String> getShardMetaData(String shardName) throws Exception;
  

  /**
   * Release all resources. No further calls will be made after this one.
   */
  public abstract void shutdown() throws IOException;
  
}
