package net.sf.katta.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Version of {@link One2ManyListMap} using concurrent access-safe collections.
 *
 * @param <K>
 * @param <V>
 */
public class ConcurrentOne2ManyListMap<K, V> implements Serializable {
  private static final long serialVersionUID = 1L;
  protected final ConcurrentHashMap<K, List<V>> _map;

  public ConcurrentOne2ManyListMap() {
    _map = new ConcurrentHashMap<K, List<V>>();
  }

  public void add(K key) {
    getOrCreateList(key);
  }

  public void add(K key, V value) {
    getOrCreateList(key).add(value);
  }

  public void addAll(K key, Collection<V> newValues) {
    getOrCreateList(key).addAll(newValues);
  }
  
  private List<V> getOrCreateList(K key) {
    List<V> list = _map.get(key);
    if (list == null) {
      list = new CopyOnWriteArrayList<V>();
      List<V> oldList = _map.putIfAbsent(key, list);
      if (oldList != null) {
        return oldList;
      }
    }
    return list;
  }

  /**
   * @param key
   * @return the list to the given key or an empty list
   */
  public List<V> getValues(K key) {
    List<V> values = _map.get(key);
    if (values == null) {
      values = Collections.emptyList();
    }
    return values;
  }

  /**
   * Removes the value from the given key list.
   * 
   * @param key
   * @param value
   * @return if value has exists or not
   */
  public boolean removeValue(K key, V value) {
    List<V> values = _map.get(key);
    if (values == null) {
      return false;
    }

    boolean success = values.remove(value);
    if (values.size() == 0) {
      _map.remove(key);
    }

    return success;
  }

  /**
   * @param key
   * @return a list with all values belonging to the given key
   */
  public List<V> removeKey(K key) {
    List<V> list = getValues(key);
    _map.remove(key);
    return list;
  }

  /**
   * @param key
   * @return true if map contains the key
   */
  public boolean containsKey(K key) {
    return _map.containsKey(key);
  }

  /**
   * 
   * @param value
   * @return true if map contains the value
   */
  public boolean containsValue(V value) {
    Collection<List<V>> values = _map.values();
    for (List<V> list : values) {
      if (list.contains(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @see ConcurrentHashMap#keySet()
   * @return a set view of the keys contained in this map.
   */
  public Set<K> keySet() {
    return _map.keySet();
  }

  /**
   * @see ConcurrentHashMap#size()
   * @return the size of the map
   */
  public int size() {
    return _map.size();
  }

  public Map<K, List<V>> asMap() {
    return _map;
  }

  @Override
  public String toString() {
    return _map.toString();
  }
}
