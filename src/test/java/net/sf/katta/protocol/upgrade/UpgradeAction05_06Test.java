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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import net.sf.katta.AbstractZkTest;
import net.sf.katta.operation.master.IndexReinitializeOperation;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.upgrade.UpgradeAction05_06.WriteableZkSerializer;
import net.sf.katta.testutil.Mocks;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

public class UpgradeAction05_06Test extends AbstractZkTest {

  @Test
  @SuppressWarnings("deprecation")
  public void testPreserveIndices() throws Exception {
    ZkClient zkClientForWriables = new ZkClient(_zk.getZkConf().getZKServers(), 5000, 5000, new WriteableZkSerializer(
            net.sf.katta.index.IndexMetaData.class));
    String indexName = "index1";
    net.sf.katta.index.IndexMetaData oldIndexMD = new net.sf.katta.index.IndexMetaData("indexPath", "analyzer", 2,
            net.sf.katta.index.IndexMetaData.IndexState.DEPLOYED);
    String oldIndicesPath = UpgradeAction05_06.getOldIndicesPath(_zk.getZkConf());
    zkClientForWriables.createPersistent(oldIndicesPath);
    zkClientForWriables.createPersistent(oldIndicesPath + "/" + indexName, oldIndexMD);
    zkClientForWriables.close();

    UpgradeAction05_06 upgradeAction = new UpgradeAction05_06();
    upgradeAction.upgrade(_protocol);

    assertEquals(1, _protocol.getIndices().size());
    IndexMetaData newIndexMD = _protocol.getIndexMD(indexName);
    assertEquals(indexName, newIndexMD.getName());
    assertEquals(oldIndexMD.getPath(), newIndexMD.getPath());
    assertEquals(oldIndexMD.getReplicationLevel(), newIndexMD.getReplicationLevel());

    MasterQueue queue = _protocol.publishMaster(Mocks.mockMaster());
    assertEquals(1, queue.size());
    assertThat(queue.peek(), instanceOf(IndexReinitializeOperation.class));
  }
}
