package com.yahoo.zookeeper.server;

import java.io.IOException;
import java.net.ServerSocket;

import net.sf.katta.util.Logger;

/**
 * Running all unit tests together produces some situations where the shutdown
 * does not frees it port. This class fixes the freeing of the port.
 * 
 */
public class ZkNioFactory extends NIOServerCnxn.Factory {

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
            Logger.error("could not close socket/selector", e);
          }
        }
      }
    } catch (final InterruptedException e) {
      Logger.error("Failed to wait for zookeeper server release port", e);
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
