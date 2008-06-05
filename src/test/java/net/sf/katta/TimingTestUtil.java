package net.sf.katta;

import net.sf.katta.util.KattaException;
import net.sf.katta.zk.ZKClient;

public class TimingTestUtil {
  public static void waitFor(final ZKClient client, final String path, final int size) throws InterruptedException,
  KattaException {
    int count = 0;
    while (client.getChildren(path).size() != size && count++ < 100) {
      Thread.sleep(1000);
    }

  }

  public static void waitFor(final ZKClient client, final String path) throws KattaException, InterruptedException {
    int count = 0;
    while (!client.exists(path) && count++ < 100) {
      Thread.sleep(1000);
    }
  }
}
