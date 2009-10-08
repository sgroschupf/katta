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
package net.sf.katta.metrics;

import java.io.Serializable;

import junit.framework.TestCase;
import net.sf.katta.monitor.JmxMonitor;
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
