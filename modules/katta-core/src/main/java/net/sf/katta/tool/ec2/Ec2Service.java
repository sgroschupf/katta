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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.GroupDescription;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

public abstract class Ec2Service {

  private Jec2 _ec2;
  private String _awsAccountId;
  private String _startAIM;
  private String _keyName;
  private String _keyPath;

  public Ec2Service(String awsAccountId, String accessKeyId, String secretAccessKey, String aim, String keyName,
          String keyPath) {
    _ec2 = new Jec2(accessKeyId, secretAccessKey);
    _awsAccountId = awsAccountId;
    _startAIM = aim;
    _keyName = keyName;
    _keyPath = keyPath;
  }

  /**
   * Lists all running clusters
   */
  public Set<String> list() throws IOException {
    HashSet<String> set = new HashSet<String>();
    try {
      List<ReservationDescription> reservations = _ec2.describeInstances(new ArrayList<String>());
      for (ReservationDescription reservation : reservations) {
        List<Instance> instances = reservation.getInstances();
        for (Instance instance : instances) {
          if ("running".equals(instance.getState())) {
            List<String> groups = reservation.getGroups();
            for (String groupName : groups) {
              groupName = groupName.replaceAll("-master", "");
              set.add(groupName);
            }
          }
        }
      }
    } catch (EC2Exception e) {
      throw new IOException("Unable to get Instances", e);
    }
    return set;
  }

  /**
   * launches a cluster
   * 
   * @param conductorHostName
   */
  public void launchCluster(String groupName, int numOfSlaves) throws IOException {
    Ec2Instance master = launchMaster(groupName);
    Ec2Instance[] nodes = launchNodes(groupName, numOfSlaves);
    postLaunch(master, nodes);
  }

  protected abstract void postLaunch(Ec2Instance master, Ec2Instance[] nodes);

  public void terminiateCluster(String cluster) throws IOException {
    String clusterMaster = getMasterGroupName(cluster);
    try {

      List<String> toTerminate = new ArrayList<String>();
      List<ReservationDescription> describeInstances;
      describeInstances = _ec2.describeInstances(new ArrayList<String>());
      for (ReservationDescription description : describeInstances) {
        List<Instance> instances = description.getInstances();
        for (Instance instance : instances) {
          if ("running".equals(instance.getState())
                  && (description.getGroups().contains(cluster) || description.getGroups().contains(clusterMaster))) {
            toTerminate.add(instance.getInstanceId());
          }
        }
      }
      _ec2.terminateInstances(toTerminate);

      // ec2-revoke $CLUSTER_MASTER -o $CLUSTER -u $AWS_ACCOUNT_ID
      _ec2.revokeSecurityGroupIngress(clusterMaster, cluster, _awsAccountId);

      // ec2-revoke $CLUSTER -o $CLUSTER_MASTER -u $AWS_ACCOUNT_ID
      _ec2.revokeSecurityGroupIngress(cluster, clusterMaster, _awsAccountId);

      // ec2-delete-group $CLUSTER_MASTER
      _ec2.deleteSecurityGroup(clusterMaster);

      // ec2-delete-group $CLUSTER
      _ec2.deleteSecurityGroup(cluster);

    } catch (EC2Exception e) {
      throw new IOException("Unable to terminate instances.", e);
    }

  }

  private Ec2Instance launchMaster(String nodeGroupName) throws IOException {
    String masterGroupName = getMasterGroupName(nodeGroupName);
    if (masterAlreadyRunning(masterGroupName)) {
      throw new IOException("Master already running");
    }

    try {
      // creating group cluster master
      if (!groupExists(masterGroupName)) {
        _ec2.createSecurityGroup(masterGroupName, "Group for Hadoop Master.");
        _ec2.authorizeSecurityGroupIngress(masterGroupName, masterGroupName, _awsAccountId);
        _ec2.authorizeSecurityGroupIngress(masterGroupName, "tcp", 22, 22, "0.0.0.0/0");

      }
      // create group cluster
      if (!groupExists(nodeGroupName)) {
        _ec2.createSecurityGroup(nodeGroupName, "Group for Hadoop Slaves.");
        _ec2.authorizeSecurityGroupIngress(nodeGroupName, nodeGroupName, _awsAccountId);
        _ec2.authorizeSecurityGroupIngress(nodeGroupName, "tcp", 22, 22, "0.0.0.0/0");
        // couple groups with each other
        _ec2.authorizeSecurityGroupIngress(masterGroupName, nodeGroupName, _awsAccountId);
        _ec2.authorizeSecurityGroupIngress(nodeGroupName, masterGroupName, _awsAccountId);
      }
      // launch master
      List<String> groupSet = new ArrayList<String>();
      groupSet.add(masterGroupName);

      ReservationDescription master = _ec2.runInstances(_startAIM, 1, 1, groupSet, null, _keyName);

      // polling until started wait at least 5 min
      Ec2Instance masterInfo = waitForMaster(master, 5 * 60 * 1000);
      if (masterInfo == null) {
        throw new IOException("Master instance " + master + " did not boot up in time");
      }
      // try to ssh into it
      long end = System.currentTimeMillis() + 1000 * 60 * 10;
      while (true) {
        if (end < System.currentTimeMillis()) {
          throw new IOException("Unable to ssh into master within 3 min.");
        }
        if (SshUtil.sshRemoteCommand(masterInfo.getExternalHost(), "echo \"hello\"", _keyPath)) {
          break;
        }
      }

      // scp private key to server
      SshUtil.scp(_keyPath, _keyPath, masterInfo.getExternalHost(), "/root/.ssh/id_rsa");
      // change mod of keyfile
      SshUtil.sshRemoteCommand(masterInfo.getExternalHost(), "chmod 600 /root/.ssh/id_rsa", _keyPath);

      return masterInfo;
    } catch (EC2Exception e) {
      throw new IOException("Unable to interact with AWS", e);
    }

  }

  private Ec2Instance[] launchNodes(String cluster, int numOfSlaves) throws IOException {
    try {
      List<String> groupSet = new ArrayList<String>();
      groupSet.add(cluster);
      _ec2.runInstances(_startAIM, numOfSlaves, numOfSlaves, groupSet, null, _keyName);
      return waitUntilStarted(cluster, 5 * 60 * 1000);
    } catch (EC2Exception e) {
      throw new IOException("Unable to create slaves", e);
    }

  }

  private String getMasterGroupName(String cluster) {
    return cluster + "-master";
  }

  private Ec2Instance waitForMaster(ReservationDescription master, int waitTime) throws EC2Exception {
    long end = System.currentTimeMillis() + waitTime;

    ArrayList<String> instanceIds = new ArrayList<String>();
    List<Instance> instances = master.getInstances();
    for (Instance instance : instances) {
      instanceIds.add(instance.getInstanceId());
    }

    while (System.currentTimeMillis() < end) {
      List<ReservationDescription> describeInstances = _ec2.describeInstances(instanceIds);
      for (ReservationDescription reservationDescription : describeInstances) {
        List<Instance> startingInstances = reservationDescription.getInstances();
        for (Instance instance : startingInstances) {
          if ("running".equals(instance.getState())) {
            return new Ec2Instance(instance.getPrivateDnsName(), instance.getDnsName());
          }
        }
      }
    }
    return null;
  }

  private boolean groupExists(String name) {
    try {
      List<GroupDescription> groups = _ec2.describeSecurityGroups(new String[] {});
      for (GroupDescription groupDescription : groups) {
        if (groupDescription.getName().equals(name)) {
          return true;
        }
      }
    } catch (EC2Exception e) {
      // ignoring in this special case...
    }
    return false;
  }

  private boolean masterAlreadyRunning(String clusterMaster) throws IOException {
    try {
      List<ReservationDescription> reservations = _ec2.describeInstances(new ArrayList<String>());
      for (ReservationDescription reservation : reservations) {
        if (reservation.getGroups().contains(clusterMaster)) {
          List<Instance> instances = reservation.getInstances();
          for (Instance instance : instances) {
            if ("running".equals(instance.getState())) {
              return true;
            }
          }
        }
      }
    } catch (EC2Exception e) {
      throw new IOException("Unable to get Instances", e);
    }
    return false;
  }

  private Ec2Instance[] waitUntilStarted(String cluster, long waitForMs) throws IOException {
    try {
      String clusterMaster = getMasterGroupName(cluster);
      long end = System.currentTimeMillis() + waitForMs;
      boolean pending = true;
      while (pending && System.currentTimeMillis() < end) {
        pending = false;
        ArrayList<Ec2Instance> list = new ArrayList<Ec2Instance>();
        List<ReservationDescription> reservations = _ec2.describeInstances(new ArrayList<String>());
        for (ReservationDescription reservation : reservations) {
          if (reservation.getGroups().contains(cluster) || reservation.getGroups().contains(clusterMaster)) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
              if ("pending".equals(instance.getState())) {
                pending = true;
                break;
              } else if ("running".equals(instance.getState())){
                list.add(new Ec2Instance(instance.getPrivateDnsName(), instance.getDnsName()));
              }
            }
            if (pending) {
              break;
            }
          }
        }
        if (!pending) {
          // nothing pending anymore.
          return list.toArray(new Ec2Instance[list.size()]);
        }
      }
      throw new IOException("Not all instances booted within given time");

    } catch (EC2Exception e) {
      throw new IOException("Unable to retrive instances", e);
    }

  }

}
