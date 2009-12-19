package net.sf.katta;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.ZkTestSystem;

import org.junit.Rule;

public class AbstractZkTest {

  @Rule
  public ZkTestSystem _zk = ZkTestSystem.getInstance();
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  protected InteractionProtocol _protocol = _zk.getInteractionProtocol();
}
