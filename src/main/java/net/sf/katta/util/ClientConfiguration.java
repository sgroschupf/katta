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
package net.sf.katta.util;

/**
 * 
 * Configuration for a {@link Client}.
 * 
 * <p>
 * <b>RPC Configuration</b><br>
 * For the client-node interaction hadoop rpc is used as the underlying
 * communication technology. <br>
 * To fine tune the communication you can also set hadoop rpc properties (like
 * 'ipc.client.connect.max.retries'). <br>
 * Those properties will be propagated to hadoop.
 * 
 */
@SuppressWarnings("serial")
public class ClientConfiguration extends KattaConfiguration {

  public final static String CLIENT_NODE_INTERACTION_MAXTRYCOUNT = "client.node.interaction.maxTryCount";

  public ClientConfiguration() {
    super();
    // set default values
    setProperty(CLIENT_NODE_INTERACTION_MAXTRYCOUNT, 3);

    // default values for underlying hadoop rpc (used for searching on nodes)
    setProperty("ipc.client.connect.max.retries", 2);
  }

}
