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
package net.sf.katta;

import net.sf.katta.protocol.metadata.IndexMetaData;

import org.junit.Test;

public class KattaTest extends AbstractZkTest {

  @Test
  public void testShowStructure() throws Exception {
    Katta.SHOW_STRUCTURE_COMMAND.execute(_zk.getZkConf(), new String[] { "showStructure" });
    Katta.SHOW_STRUCTURE_COMMAND.execute(_zk.getZkConf(), new String[] { "showStructure", "-d" });
  }

  @Test(expected = Exception.class)
  public void testShowErrorsWithIndexNotExist() throws Exception {
    Katta.LIST_ERRORS_COMMAND.execute(_zk.getZkConf(), new String[] { "listErrors", "a" });
  }

  @Test
  public void testListIndexesWithUnreachableIndex_KATTA_76() throws Exception {
    IndexMetaData indexMD = new IndexMetaData("indexABC", "hdfs://localhost:8020/unreachableIndex", 1);
    _zk.getInteractionProtocol().publishIndex(indexMD);
    Katta.LIST_INDICES_COMMAND.execute(_zk.getZkConf(), new String[] { indexMD.getName() });
  }

}
