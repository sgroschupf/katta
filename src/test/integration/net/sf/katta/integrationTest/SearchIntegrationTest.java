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
package net.sf.katta.integrationTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.client.Client;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.StringUtil;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.net.DNS;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;

public class SearchIntegrationTest extends TestCase {

  public void testSearch() throws Exception {
    long startTime = System.currentTimeMillis();
    ZkConfiguration conf = new ZkConfiguration();
    FileUtil.deleteFolder(conf.getZKDataDir());
    FileUtil.deleteFolder(conf.getZKDataLogDir());
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());

    // start katta cluster
    KattaMiniCluster miniCluster = new KattaMiniCluster(conf, 5);
    miniCluster.start();

    // deploy indexes
    miniCluster.deployTestIndexes(TestResources.INDEX1, KeywordAnalyzer.class, 3, 3);

    // start search threads
    SearchThread searchThread = new SearchThread(12);
    searchThread.start();

    int queryTime = 5000;
    Thread.sleep(queryTime);
    searchThread.interrupt();
    searchThread.join();

    // stop everything
    miniCluster.stop();

    // print results
    System.out.println("===========================================");
    System.out.println("search time: " + StringUtil.formatTimeDuration(queryTime));
    System.out.println("fired queries: " + searchThread.getFiredQueryCount());
    System.out.println("wrong results: " + searchThread.getUnexpectedResultCount());
    System.out.println("exceptions: " + searchThread.getThrownExceptions().size());
    System.out.println("execution took: " + StringUtil.formatTimeDuration(System.currentTimeMillis() - startTime));
    System.out.println("===========================================");

    assertEquals("unexpected hit count", 0, searchThread.getUnexpectedResultCount());
    assertEquals("exceptions on search", 0, searchThread.getThrownExceptions().size());
    // TODO jz: add assertions
  }

  public static void main(String[] args) throws IOException {
    System.out.println(Integer.MAX_VALUE);
    System.out.println(Arrays.asList(DNS.getHosts("default")));

    ServerSocket serverSocket = new ServerSocket(0);
    Socket socket = new Socket();
    socket.setSoTimeout(2000);
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost().getHostName(), serverSocket.getLocalPort()));
    System.out.println(InetAddress.getLocalHost().getHostName());
    System.out.println(InetAddress.getLocalHost());
    System.out.println(socket.getInetAddress());
    Socket accept = serverSocket.accept();
    System.out.println(accept.getRemoteSocketAddress());

    System.out.println();
    final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
    for (final Enumeration ifaces = networkInterfaces; ifaces.hasMoreElements();) {
      final NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
      System.out.println(iface.getDisplayName());
      for (final Enumeration ips = iface.getInetAddresses(); ips.hasMoreElements();) {
        InetAddress ia = (InetAddress) ips.nextElement();
        System.out.println(ia.getCanonicalHostName());
        System.out.println(InetAddress.getByAddress(ia.getAddress()));
      }
    }
  }

  protected static class SearchThread extends Thread {

    private static Logger LOG = Logger.getLogger(SearchThread.class);

    private volatile boolean _stopped;
    private final long _expectedTotalHitCount;

    private List<Exception> _thrownExceptions = new ArrayList<Exception>();
    private long _firedQueryCount;
    private long _unexpectedResultCount;

    public SearchThread(long expectedTotalHitCount) {
      _expectedTotalHitCount = expectedTotalHitCount;
    }

    @Override
    public void run() {
      try {
        Client client = new Client();
        while (!_stopped) {
          Hits hits = client.search(new Query("foo:bar"), new String[] { "*" });
          _firedQueryCount++;
          if (hits.size() != _expectedTotalHitCount) {
            _unexpectedResultCount++;
            LOG.warn("expected " + _expectedTotalHitCount + " hits but got " + hits.size());
          }
        }
        client.close();
      } catch (KattaException e) {
        if (!(e.getCause() instanceof InterruptedException)) {
          e.printStackTrace();
          _thrownExceptions.add(e);
        }
      }
    }

    @Override
    public void interrupt() {
      _stopped = true;
      // jz: we don't call super.interrupt() since the client swallows such
      // InterruptedException's
    }

    public List<Exception> getThrownExceptions() {
      return _thrownExceptions;
    }

    public long getFiredQueryCount() {
      return _firedQueryCount;
    }

    public long getUnexpectedResultCount() {
      return _unexpectedResultCount;
    }

  }
}
