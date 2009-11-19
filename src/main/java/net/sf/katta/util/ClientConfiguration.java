package net.sf.katta.util;

import net.sf.katta.client.Client;

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
