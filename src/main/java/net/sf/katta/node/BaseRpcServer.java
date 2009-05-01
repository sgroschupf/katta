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
package net.sf.katta.node;

import java.io.IOException;
import java.net.BindException;

import net.sf.katta.util.NetworkUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

public abstract class BaseRpcServer {

  private static final Logger LOG = Logger.getLogger(BaseRpcServer.class);

  private Server _rpcServer;
  private int _serverPort;
  private String _hostName;

  /**
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range from startPort to startPort + 10000
   * 
   * @param startPort
   *            The start port.
   * @return The network address of the server.
   */
  public final String startRpcServer(int startPort) {
    int serverPort = startPort;
    _hostName = NetworkUtil.getLocalhostName();
    int tryCount = 10000;
    while (_rpcServer == null) {
      try {
        _rpcServer = RPC.getServer(this, "0.0.0.0", serverPort, new Configuration());
        LOG.info("Search server started on : " + _hostName + ":" + serverPort);
        _serverPort = serverPort;
      } catch (final BindException e) {
        if (serverPort - startPort < tryCount) {
          serverPort++;
          // try again
        } else {
          throw new RuntimeException("Tried " + tryCount + " ports and none is free...");
        }
      } catch (final IOException e) {
        throw new RuntimeException("Unable to create rpc search server", e);
      }
    }

    setup();

    try {
      _rpcServer.start();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start rpc search server", e);
    }
    return _hostName + ":" + serverPort;
  }

  protected void stopRpcServer() {
    if (_rpcServer != null) {
      _rpcServer.stop();
      _rpcServer = null;
    }
  }

  public void join() throws InterruptedException {
    _rpcServer.join();
  }

  protected int getRpcServerPort() {
    return _serverPort;
  }

  protected String getRpcHostName() {
    return _hostName;
  }
  
  public Server getRpcServer() {
    return _rpcServer;
  }

  protected abstract void setup();
}
