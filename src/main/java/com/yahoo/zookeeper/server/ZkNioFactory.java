package com.yahoo.zookeeper.server;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

/**
 * Running all unit tests together produces some situations where the shutdown
 * does not frees it port. This class fixes the freeing of the port.
 * 
 */
public class ZkNioFactory extends NIOServerCnxn.Factory {

  private final static Logger LOG = Logger.getLogger(ZkNioFactory.class);

  private final int _port;

  public ZkNioFactory(int port) throws IOException {
    super(port);
    _port = port;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    try {
      int waitedSeconds = 0;
      while (!isPortFree(_port)) {
        Thread.sleep(1000);
        waitedSeconds += 1;
        if (waitedSeconds > 2) {
          try {
            ss.socket().close();
            selector.close();
          } catch (IOException e) {
            LOG.error("could not close socket/selector", e);
          }
        }
      }
    } catch (final InterruptedException e) {
      LOG.error("Failed to wait for zookeeper server release port", e);
    }

  }

  private boolean isPortFree(final int port) {
    try {
      final ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (final Exception e) {
      return false;
    }
  }
}
