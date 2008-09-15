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
package net.sf.katta.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class Gateway extends Thread {

  protected static Logger LOG = Logger.getLogger(Gateway.class);

  private final int _port;
  private final int _destinationPort;
  private ServerSocket _serverSocket;

  protected List<Thread> _runningThreads = new ArrayList<Thread>();

  public Gateway(int port, int destinationPort) {
    _port = port;
    _destinationPort = destinationPort;
  }

  public void run() {
    try {
      LOG.info("starting gateway on port " + _port + " pointing to port " + _destinationPort);
      _serverSocket = new ServerSocket(_port);
      while (true) {
        final Socket socket = _serverSocket.accept();
        LOG.info("new client is connected " + socket.getInetAddress());
        final InputStream incomingInputStream = socket.getInputStream();
        final OutputStream incomingOutputStream = socket.getOutputStream();

        final Socket outgoingSocket;
        try {
          outgoingSocket = new Socket("localhost", _destinationPort);
        } catch (Exception e) {
          LOG.warn("could not connect to " + _destinationPort);
          continue;
        }
        final InputStream outgoingInputStream = outgoingSocket.getInputStream();
        final OutputStream outgoingOutputStream = outgoingSocket.getOutputStream();

        Thread writeThread = new Thread() {
          public void run() {
            try {
              int read = -1;
              while ((read = incomingInputStream.read()) != -1) {
                outgoingOutputStream.write(read);
              }
            } catch (IOException e) {
              //
            }
            LOG.info("write thread terminated");
            synchronized (_runningThreads) {
              _runningThreads.remove(this);
            }
          }

          @Override
          public void interrupt() {
            try {
              socket.close();
              outgoingSocket.close();
            } catch (IOException e) {
              LOG.error("error on stopping closing sockets", e);
            }

            super.interrupt();
          }
        };

        Thread readThread = new Thread() {
          public void run() {
            try {
              int read = -1;
              while ((read = outgoingInputStream.read()) != -1) {
                incomingOutputStream.write(read);
              }
            } catch (IOException e) {
              //
            }
            LOG.info("read thread terminated");
            synchronized (_runningThreads) {
              _runningThreads.remove(this);
            }
          }
        };

        writeThread.setDaemon(true);
        readThread.setDaemon(true);
        writeThread.start();
        readThread.start();
        _runningThreads.add(writeThread);
        _runningThreads.add(readThread);
      }
    } catch (SocketException e) {
      LOG.info("stopping gateway");
      synchronized (_runningThreads) {
        List<Thread> runningThreads = _runningThreads;
        for (Thread thread : runningThreads) {
          thread.interrupt();
        }
      }
    } catch (Exception e) {
      LOG.error("error on gateway execution", e);
    }
  }

  @Override
  public void interrupt() {
    try {
      _serverSocket.close();
    } catch (Exception cE) {
      LOG.error("error on stopping gateway", cE);
    }
    super.interrupt();
  }

  public void interruptAndJoin() throws InterruptedException {
    interrupt();
    join();
  }

}
