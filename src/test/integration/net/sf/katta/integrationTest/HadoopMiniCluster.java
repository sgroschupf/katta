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

import net.sf.katta.util.IHadoopConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.log4j.Logger;

public class HadoopMiniCluster {

  private static final Logger LOG = Logger.getLogger(HadoopMiniCluster.class);
  private final int _namenodePort;
  private final int _jobtrackerPort;
  private final int _datanodeCount;
  private final int _tasktrackerCount;

  private MiniDFSCluster _dfsCluster;
  private MiniMRCluster _mrCluster;

  public HadoopMiniCluster(int namenodePort, int jobtrackerPort, int datanodeCount, int tasktrackerCount) {
    _namenodePort = namenodePort;
    _jobtrackerPort = jobtrackerPort;
    _datanodeCount = datanodeCount;
    _tasktrackerCount = tasktrackerCount;
  }

  public void start() throws IOException {
    LOG.info("starting hadoop cluster...");
    Configuration conf = new Configuration();
    System.setProperty("test.build.data", "build");
    System.setProperty("hadoop.log.dir", "build/logs");

    _dfsCluster = new MiniDFSCluster(_namenodePort, conf, _datanodeCount, true, true, StartupOption.REGULAR, null);
    LOG.info("started namnode on " + conf.get(IHadoopConstants.NAMENODE));

    _mrCluster = new MiniMRCluster(_jobtrackerPort, 0, _tasktrackerCount, conf.get(IHadoopConstants.NAMENODE), 1);
    LOG.info("started jobtracker on " + _mrCluster.createJobConf().get(IHadoopConstants.JOBTRACKER));
  }

  public JobConf createJobConf() {
    if (_mrCluster == null) {
      throw new IllegalStateException("cluster is not started yet...");
    }
    return _mrCluster.createJobConf();
  }

  public void stop() {
    LOG.info("stopping hadoop cluster...");
    _mrCluster.shutdown();
    _dfsCluster.shutdown();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    HadoopMiniCluster hadoopCluster = new HadoopMiniCluster(9000, 9001, 2, 4);
    hadoopCluster.start();
    Thread.sleep(2000);
    hadoopCluster.stop();
  }
}
