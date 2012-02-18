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
package net.sf.katta.tool.ec2;

import java.util.Set;

import junit.framework.Assert;
import net.sf.katta.AbstractTest;

import org.junit.Test;

public class Ec2ServiceTest extends AbstractTest {

  @Test
  public void testLaunchCluster() throws Exception {

    // TODO this is actually more an integration test
    if (Ec2ServiceTest.class.getResource("/katta.ec2.properties") == null) {
      System.err.println("Ec2ServiceTest did not run since property file is missing");
      return;
    }
    Ec2Configuration ec2Config = new Ec2Configuration();

    String awsAccountId = ec2Config.getAccountId();
    String aWSAccessKeyId = ec2Config.getAccessKey();
    String secretAccessKey = ec2Config.getSecretAccessKey();
    String keyName = ec2Config.getKeyName();
    String keyPath = ec2Config.getKeyPath();
    String aim = ec2Config.getAim();

    DummyEc2Service hadoop = new DummyEc2Service(awsAccountId, aWSAccessKeyId, secretAccessKey, aim, keyName, keyPath);
    String cluster = "kattaTestCluster" + System.currentTimeMillis();
    int numOfSlaves = 1;

    hadoop.launchCluster(cluster, numOfSlaves);
    Set<String> clusters = hadoop.list();

    Assert.assertTrue(clusters.contains(cluster));

    // should we create a tunnel and run some queries here?

    for (String clusterName : clusters) {
      hadoop.terminiateCluster(clusterName);
    }

  }

  private class DummyEc2Service extends Ec2Service {

    public DummyEc2Service(String awsAccountId, String accessKeyId, String secretAccessKey, String aim, String keyName,
            String keyPath) {
      super(awsAccountId, accessKeyId, secretAccessKey, aim, keyName, keyPath);
    }

    @Override
    protected void postLaunch(Ec2Instance master, Ec2Instance[] nodes) {
      if (master == null || nodes == null || nodes.length == 0) {
        throw new IllegalArgumentException("Master or Nodes can't be empthy");
      }
    }
  }
}
