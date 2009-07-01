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
package net.sf.katta.zk;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;
import net.sf.katta.util.ZkConfiguration;

public class ZkPathsTest extends TestCase {

//  private ZkConfiguration config = new ZkConfiguration();
  
  String node1 = "node1:20000";
  String node2 = "node2:20000";
  String index1 = "index1";
  String shard1 = index1 + "_1";
  String shard2 = index1 + "_2";


  public void testDefaultRootPaths() {
    ZkConfiguration config = new ZkConfiguration();
    assertEquals("/katta", config.getZKRootPath());
    assertEquals("/katta/master", config.getZKMasterPath());
    assertEquals("/katta/nodes", config.getZKNodesPath());
    assertEquals("/katta/indexes", config.getZKIndicesPath());
    assertEquals("/katta/node-to-shard", config.getZKNodeToShardPath());
    assertEquals("/katta/shard-to-node", config.getZKShardToNodePath());
    assertEquals("/katta/shard-to-error", config.getZKShardToErrorPath());
    config = withRoot(null);
    assertEquals("/katta", config.getZKRootPath());
    assertEquals("/katta/master", config.getZKMasterPath());
    assertEquals("/katta/nodes", config.getZKNodesPath());
    assertEquals("/katta/indexes", config.getZKIndicesPath());
    assertEquals("/katta/node-to-shard", config.getZKNodeToShardPath());
    assertEquals("/katta/shard-to-node", config.getZKShardToNodePath());
    assertEquals("/katta/shard-to-error", config.getZKShardToErrorPath()); }
  
  public void testDefaultRootPaths2() throws Exception {
    runTests(new ZkConfiguration());
  }
  
  public void testEmptyRootPath() throws Exception {
    runTests(withRoot("/"));
  }
  
  public void testSingleElementPath() throws Exception {
    runTests(withRoot("/katta"));
    runTests(withRoot("/test"));
    runTests(withRoot("/this-is-a-test"));
  }
  
  public void testMultiElementPath() throws Exception {
    runTests(withRoot("/a/b"));
    runTests(withRoot("/this/is/a/test"));
    runTests(withRoot("/katta20090513080000/mapfile"));
    runTests(withRoot("/a/b/c/d/e/f/g"));
  }
  
  private ZkConfiguration withRoot(String rootPath) {
    Properties props = new Properties();
    if (rootPath != null) {
      props.setProperty(ZkConfiguration.ZOOKEEPER_ROOT_PATH, rootPath);
    }
    return new ZkConfiguration(props, null);
  }
  
  private void runTests(ZkConfiguration config) throws Exception {
    getNodePath(config);
    getIndexPath(config);
    getShard2NodePath(config);
    getZKNodeToShardPath(config);
    getName(config);
  }

    
  private void getNodePath(ZkConfiguration config) throws Exception {
    assertEquals(config.getZKNodesPath() + "/" + node1, config.getZKNodePath(node1));
    assertFalse(config.getZKNodePath(node1).equals(config.getZKNodePath(node2)));
  }

  private void getIndexPath(ZkConfiguration config) throws Exception {
    assertEquals(config.getZKIndicesPath() + "/" + index1, config.getZKIndexPath(index1));
    assertFalse(config.getZKIndexPath(node1).equals(config.getZKIndexPath("index2")));
  }

  private void getShard2NodePath(ZkConfiguration config) throws Exception {
    assertEquals(config.getZKShardToNodePath() + "/" + shard1 + "/" + node1, config.getZKShardToNodePath(shard1, node1));
    assertEquals(config.getZKShardToNodePath() + "/" + shard1 + "/" + node2, config.getZKShardToNodePath(shard1, node2));
    assertEquals(config.getZKShardToNodePath() + "/" + shard2 + "/" + node1, config.getZKShardToNodePath(shard2, node1));

    assertEquals(new File(config.getZKShardToNodePath(shard1)).getAbsolutePath(), 
                 new File(config.getZKShardToNodePath(shard1, node1)).getParentFile().getAbsolutePath());
  }

  private void getZKNodeToShardPath(ZkConfiguration config) throws Exception {
    assertEquals(config.getZKNodeToShardPath() + "/" + node1 + "/" + shard1, config.getZKNodeToShardPath(node1, shard1));
    assertEquals(config.getZKNodeToShardPath() + "/" + node1 + "/" + shard2, config.getZKNodeToShardPath(node1, shard2));
    assertEquals(config.getZKNodeToShardPath() + "/" + node2 + "/" + shard1, config.getZKNodeToShardPath(node2, shard1));

    assertEquals(new File(config.getZKNodeToShardPath(node1)).getAbsolutePath(),
            new File(config.getZKNodeToShardPath(node1, shard1)).getParentFile().getAbsolutePath());
    assertFalse(config.getZKNodeToShardPath(node1, shard1).equals(config.getZKShardToNodePath(shard1, node1)));
  }

  private void getName(ZkConfiguration config) throws Exception {
    assertEquals(node1, config.getZKName(config.getZKNodeToShardPath(node1)));
  }
  

  public void testInvalidRootPath() {
    assertEquals("/katta", withRoot(null).getZKRootPath());
    assertEquals("/", withRoot("").getZKRootPath());
    assertEquals("/", withRoot("/").getZKRootPath());
    assertEquals("/katta", withRoot("katta").getZKRootPath());
    assertEquals("/katta", withRoot("katta/").getZKRootPath());
    assertEquals("/katta/test", withRoot("katta/test").getZKRootPath());
  }
  
  public void testSetRootPath() {
    ZkConfiguration conf = new ZkConfiguration();
    assertEquals("/katta", conf.getZKRootPath());
    conf.setZKRootPath("/lemur");
    assertEquals("/lemur", conf.getZKRootPath());
    conf.setZKRootPath("lemur");
    assertEquals("/lemur", conf.getZKRootPath());
    conf.setZKRootPath("/a/b/c");
    assertEquals("/a/b/c", conf.getZKRootPath());
    conf.setZKRootPath("");
    assertEquals("/", conf.getZKRootPath());
    conf.setZKRootPath("/lemur/");
    assertEquals("/lemur", conf.getZKRootPath());
    conf.setZKRootPath(null);
    assertEquals("/katta", conf.getZKRootPath());
  }

}
