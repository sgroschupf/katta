package net.sf.katta.util;

import junit.framework.TestCase;

public class One2ManyListMapTest extends TestCase {

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
