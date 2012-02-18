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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.katta.AbstractTest;

import org.junit.Test;

public class CollectionUtilTest extends AbstractTest {

  @Test
  public void testInvertListMap() throws Exception {
    Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
    map.put("a", Arrays.asList(1, 2, 3));
    map.put("b", Arrays.asList(2, 4));

    Map<Integer, List<String>> invertMap = CollectionUtil.invertListMap(map);
    assertEquals(4, invertMap.size());
    assertTrue(invertMap.containsKey(1));
    assertTrue(invertMap.containsKey(2));
    assertTrue(invertMap.containsKey(3));
    assertTrue(invertMap.containsKey(4));

    assertTrue(invertMap.get(1).contains("a"));
    assertTrue(invertMap.get(2).contains("a"));
    assertTrue(invertMap.get(3).contains("a"));

    assertTrue(invertMap.get(2).contains("b"));
    assertTrue(invertMap.get(4).contains("b"));

    int valueCount = 0;
    Collection<List<String>> values = invertMap.values();
    for (List<String> valueList : values) {
      valueCount += valueList.size();
    }
    assertEquals(5, valueCount);
  }

  @Test
  public void testInvertListMap_EmptyValues() throws Exception {
    Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
    map.put("a", Collections.EMPTY_LIST);
    map.put("b", Arrays.asList(2, 4));

    Map<Integer, List<String>> invertMap = CollectionUtil.invertListMap(map);
    assertEquals(2, invertMap.size());
    assertTrue(invertMap.get(2).contains("b"));
    assertTrue(invertMap.get(4).contains("b"));
  }

}
