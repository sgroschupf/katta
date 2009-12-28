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
package net.sf.katta.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import net.sf.katta.AbstractTest;

import org.junit.Test;

public class One2ManyListMapTest extends AbstractTest {

  @Test
  public void testMap() throws Exception {
    One2ManyListMap<String, Integer> multiMap = new One2ManyListMap<String, Integer>();
    assertEquals(0, multiMap.size());
    assertEquals(0, multiMap.asMap().size());
    assertTrue(multiMap.getValues("notExistentKey").isEmpty());

    multiMap.add("a", 1);
    assertEquals(1, multiMap.size());
    assertEquals(1, multiMap.asMap().size());
    assertFalse(multiMap.getValues("a").isEmpty());
    assertEquals(new Integer(1), multiMap.getValues("a").get(0));
  }
}
