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
package net.sf.katta.testutil;

import java.io.File;

public class TestResources {

  public final static File INDEX1 = new File("./src/test/testIndexA");
  public final static File INDEX2 = new File("./src/test/testIndexB");

  public final static File UNZIPPED_INDEX = new File("./src/test/testIndexC");
  public final static File INVALID_INDEX = new File("./src/test/testIndexInvalid");

  public final static File SHARD1 = new File(INDEX2, "aIndex.zip");

  public final static File MAP_FILE_A = new File("./src/test/testMapFileA");
  public final static File MAP_FILE_B = new File("./src/test/testMapFileB");
  
  /** The shards will be created at run time. */
  public final static File EMPTY1_INDEX = new File("./build/data/TestResources/empty1");
  /** The shards will be created at run time. */
  public final static File EMPTY2_INDEX = new File("./build/data/TestResources/empty2");
  
}
