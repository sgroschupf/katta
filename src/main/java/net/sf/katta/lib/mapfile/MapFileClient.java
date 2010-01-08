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
package net.sf.katta.lib.mapfile;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.client.Client;
import net.sf.katta.client.ClientResult;
import net.sf.katta.client.INodeSelectionPolicy;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * The front end to the MapFile server.
 */
public class MapFileClient implements IMapFileClient {

  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(MapFileClient.class);
  private static final long TIMEOUT = 12000;
  
  private Client kattaClient;
  
  public MapFileClient(final INodeSelectionPolicy nodeSelectionPolicy) {
    kattaClient = new Client(IMapFileServer.class, nodeSelectionPolicy);
  }

  public MapFileClient() {
    kattaClient = new Client(IMapFileServer.class);
  }

  public MapFileClient(final ZkConfiguration zkConfig) {
    kattaClient = new Client(IMapFileServer.class, zkConfig);
  }

  public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration czkCnfig) {
    kattaClient = new Client(IMapFileServer.class, policy, czkCnfig);
  }

  public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig,
      ClientConfiguration clientConfiguration) {
    kattaClient = new Client(IMapFileServer.class, policy, zkConfig, clientConfiguration);
  }


//  public List<Writable> get(WritableComparable<?> key, String[] shards) throws IOException {

  private static final Method GET_METHOD;
  private static final int GET_METHOD_SHARD_ARG_IDX = 1;
  static {
    try {
      GET_METHOD = IMapFileServer.class.getMethod("get", 
              new Class[] { Text.class, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method get() in IMapFileServer!");
    }
  }
  
  public List<String> get(final String key, final String[] indexNames) throws KattaException {
    ClientResult<TextArrayWritable> results = kattaClient.broadcastToIndices(TIMEOUT, true, GET_METHOD, GET_METHOD_SHARD_ARG_IDX, indexNames, new Text(key), null);
    if (results.isError()) {
      throw results.getKattaException();
    }
    List<String> stringResults = new ArrayList<String>();
    for (TextArrayWritable taw : results.getResults()) {
      for (Writable w : taw.array.get()) {
        Text text = (Text) w;
        stringResults.add(text.toString());
      }
    }
    return stringResults;
  }
  

  public void close() {
    kattaClient.close();
  }

}
