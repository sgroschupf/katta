package net.sf.katta.loadtest;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.util.KattaException;

public class LoadTestNodeTest extends AbstractKattaTest {

  public void testShutdown() throws KattaException {
    LoadTestNode node = startLoadTestNode();
    node.shutdown();
  }
}
