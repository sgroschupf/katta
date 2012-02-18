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
package net.sf.katta.client;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.testutil.TestResources;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeployClientZkTest extends AbstractZkTest {

  @Test
  public void testAddIndex() throws Exception {
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex(TestResources.INDEX1.getName(), TestResources.INDEX1.getAbsolutePath(), 1);
  }

  @Test
  public void testIndexAcces() throws Exception {
    IndexMetaData indexMD = new IndexMetaData("index1", "indexPath", 1);
    IDeployClient deployClient = new DeployClient(_protocol);

    assertFalse(deployClient.existsIndex(indexMD.getName()));
    assertNull(deployClient.getIndexMetaData(indexMD.getName()));
    assertEquals(0, deployClient.getIndices().size());

    _protocol.publishIndex(indexMD);
    assertTrue(deployClient.existsIndex(indexMD.getName()));
    assertNotNull(deployClient.getIndexMetaData(indexMD.getName()));
    assertEquals(1, deployClient.getIndices().size());
  }

  @Test
  public void testIndexRemove() throws Exception {
    IndexMetaData indexMD = new IndexMetaData("index1", "indexPath", 1);
    IDeployClient deployClient = new DeployClient(_protocol);

    try {
      deployClient.removeIndex(indexMD.getName());
      fail("should throw exception");
    } catch (Exception e) {
      // expected
    }

    _protocol.publishIndex(indexMD);
    deployClient.removeIndex(indexMD.getName());
    assertTrue(deployClient.existsIndex(indexMD.getName()));// undeploy
    // operation is send
    // to master
  }

}
