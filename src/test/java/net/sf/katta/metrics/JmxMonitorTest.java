package net.sf.katta.metrics;

import java.io.Serializable;

import junit.framework.TestCase;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;
import org.mockito.Mockito;

public class JmxMonitorTest extends TestCase {

  public void testGetCpu() throws Exception {
    JmxMonitor monitor = new JmxMonitor();
    ZkClient zkClient = Mockito.mock(ZkClient.class);
    ZkConfiguration zkConfiguration = Mockito.mock(ZkConfiguration.class);
    String somePath = "/somePaht";
    Mockito.when(zkConfiguration.getZKMetricsPathForServer("someId")).thenReturn(somePath);
    monitor.startMonitoring("someId", zkClient, zkConfiguration);
    Thread.sleep(1200);
    Mockito.verify(zkClient, Mockito.atLeastOnce()).writeData(Mockito.anyString(), (Serializable) Mockito.anyObject());
  }

}
