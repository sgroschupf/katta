package net.sf.katta.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

public class ConcurrentOne2ManyListMapTest {
  @Test
  public void testMap() throws Exception {
    ConcurrentOne2ManyListMap<String, Integer> multiMap = new ConcurrentOne2ManyListMap<String, Integer>();
    assertEquals(0, multiMap.size());
    assertEquals(0, multiMap.asMap().size());
    assertTrue(multiMap.getValues("notExistentKey").isEmpty());

    multiMap.add("a", 1);
    assertEquals(1, multiMap.size());
    assertEquals(1, multiMap.asMap().size());
    assertFalse(multiMap.getValues("a").isEmpty());
    assertEquals(new Integer(1), multiMap.getValues("a").get(0));
  }
  
  @Test
  public void testConcurrentAdd() throws InterruptedException {
    final ConcurrentOne2ManyListMap<Integer, Integer> multiMap = new ConcurrentOne2ManyListMap<Integer, Integer>();
    
    multiMap.add(1);
    Set<Integer> keys = multiMap.keySet();
    Iterator<Integer> iterator = keys.iterator();
    multiMap.add(2);
    iterator.next();
  }
  
  @Test
  public void testConcurrentAddEntry() throws InterruptedException {
    final ConcurrentOne2ManyListMap<Integer, Integer> multiMap = new ConcurrentOne2ManyListMap<Integer, Integer>();
    
    for (int i = 0; i < 10; i++) {
      multiMap.add(1, i);
    }
    
    Iterator<Integer> iterator = multiMap.getValues(1).iterator();
    
    for (int i = 0; i < 10; i++) {
      multiMap.add(1, i);
    }
    
    iterator.next();
  }
}
