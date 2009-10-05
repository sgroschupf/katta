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
package net.sf.katta.monitor;

import java.io.Serializable;
import java.util.List;

import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

public class MetricLogger implements IZkChildListener, IZkDataListener {

  public enum OutputType {
    Log4J, SystemOut;
  }

  private final static Logger LOG = Logger.getLogger(MetricLogger.class);

  private final ZkClient _zkClient;

  private OutputType _outputType;

  public MetricLogger(OutputType outputType, ZkClient zkClient, ZkConfiguration zkConf) {
    _outputType = outputType;
    _zkClient = zkClient;
    String zkMetricsPath = zkConf.getZKMetricsPath();
    zkClient.subscribeChildChanges(zkMetricsPath, this);
    List<String> children = zkClient.getChildren(zkMetricsPath);
    subscribeDataUpdates(zkMetricsPath, children);
  }

  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    // in case new nodes join the cluster...
    subscribeDataUpdates(parentPath, currentChilds);
  }

  private void subscribeDataUpdates(String parentPath, List<String> currentChilds) {
    for (String childName : currentChilds) {
      _zkClient.subscribeDataChanges(parentPath + "/" + childName, this);
    }

  }

  @Override
  public void handleDataChange(String dataPath, Serializable data) throws Exception {
    MetricsRecord metrics = (MetricsRecord) data;
    if (_outputType == OutputType.Log4J) {
      LOG.info(metrics);
    } else {
      System.out.println(metrics.toString());
    }
  }

  public void handleDataDeleted(String dataPath) throws Exception {
    // we actually not interested in this event.
  }

}
