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

import net.sf.katta.Katta.Command;
import net.sf.katta.protocol.metadata.IndexMetaData;

import org.junit.Test;

public class KattaTest extends AbstractZkTest {

  @Test
  public void testShowStructure() throws Exception {
    execute(Katta.SHOW_STRUCTURE_COMMAND, "showStructure");
    execute(Katta.SHOW_STRUCTURE_COMMAND, "showStructure", "-d");
  }

  @Test(expected = Exception.class)
  public void testShowErrorsWithIndexNotExist() throws Exception {
    execute(Katta.LIST_ERRORS_COMMAND, "showStructure", "a");
  }

  @Test
  public void testListIndexesWithUnreachableIndex_KATTA_76() throws Exception {
    IndexMetaData indexMD = new IndexMetaData("indexABC", "hdfs://localhost:8020/unreachableIndex", 1);
    _zk.getInteractionProtocol().publishIndex(indexMD);
    execute(Katta.LIST_INDICES_COMMAND, indexMD.getName());
  }

  private void execute(Command command, String... args) throws Exception {
    command.parseArguments(_zk.getZkConf(), args);
    command.execute(_zk.getZkConf());
  }

}
