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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import net.sf.katta.AbstractTest;
import net.sf.katta.operation.master.AbstractIndexOperation;
import net.sf.katta.operation.master.IndexDeployOperation;
import net.sf.katta.protocol.InteractionProtocol;

import org.junit.Test;

public class DeployClientTest extends AbstractTest {

  protected InteractionProtocol _protocol = mock(InteractionProtocol.class);

  @Test
  public void testAddIndex() throws Exception {
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex("i1", "iP", 1);
    verify(_protocol).addMasterOperation(any(IndexDeployOperation.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIndexWithInvalidReplicationCount() throws Exception {
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex("i1", "iP", 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIndexWithInvalidName() throws Exception {
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex("*", "iP", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddIndexWithInvalidName2() throws Exception {
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex("a" + AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR, "iP", 1);
  }
}
