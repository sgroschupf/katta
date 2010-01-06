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
package net.sf.katta.protocol.upgrade;

import java.util.HashMap;
import java.util.Map;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.Version;

import org.apache.log4j.Logger;

public class UpgradeRegistry {

  private static final Logger LOG = Logger.getLogger(UpgradeRegistry.class);
  private static Map<VersionPair, UpgradeAction> _upgradeActionsByVersion = new HashMap<VersionPair, UpgradeAction>();

  static {
    registerUpgradeAction("0.5", "0.6", new UpgradeAction05_06());
  }

  protected static void registerUpgradeAction(String fromVersion, String toVersion, UpgradeAction upgradeAction) {
    _upgradeActionsByVersion.put(new VersionPair(fromVersion, toVersion), upgradeAction);
  }

  public static UpgradeAction findUpgradeAction(InteractionProtocol protocol, Version distributionVersion) {
    Version clusterVersion = protocol.getVersion();
    if (clusterVersion == null) {
      // version exist up from 0.6 only
      boolean isPre0_6Cluster = protocol.getZkClient().exists(
              protocol.getZkConfiguration().getZkRootPath() + "/indexes");
      if (isPre0_6Cluster) {
        LOG.info("version of cluster not found - assuming 0.5");
        clusterVersion = new Version("0.5", "Unknown", "Unknown", "Unknown");
      } else {
        clusterVersion = distributionVersion;
      }
    }
    LOG.info("version of distribution " + distributionVersion.getNumber());
    LOG.info("version of cluster " + clusterVersion.getNumber());
    if (clusterVersion.equals(distributionVersion)) {
      return null;
    }

    VersionPair currentVersionPair = new VersionPair(clusterVersion.getNumber(), distributionVersion.getNumber());
    LOG.warn("cluster version differs from distribution version " + currentVersionPair);
    for (VersionPair versionPair : _upgradeActionsByVersion.keySet()) {
      LOG.info("checking upgrade action " + versionPair);
      if (currentVersionPair.getFromVersion().startsWith(versionPair.getFromVersion())
              && currentVersionPair.getToVersion().startsWith(versionPair.getToVersion())) {
        LOG.info("found matching upgrade action");
        return _upgradeActionsByVersion.get(versionPair);
      }
    }

    LOG.warn("found no upgrade action for " + currentVersionPair + " out of " + _upgradeActionsByVersion.keySet());
    return null;
  }

  protected static class VersionPair {
    private String _fromVersion;
    private String _toVersion;

    protected VersionPair(String fromVersion, String toVersion) {
      _fromVersion = fromVersion;
      _toVersion = toVersion;
    }

    public String getFromVersion() {
      return _fromVersion;
    }

    public String getToVersion() {
      return _toVersion;
    }

    @Override
    public String toString() {
      return getFromVersion() + " -> " + getToVersion();
    }
  }

}
