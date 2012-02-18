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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CircularList where added elements are always on top. <br/>
 * You can cycle through the list with getNext(), whereat the returned object is
 * moved to the end of list. <br/>
 * Also you could get the top of the list through getTop() and then move it to
 * end with moveToEnd() or to the middle of this list with moveToMid().
 * 
 * Note: This implementation is not synchronized.
 * 
 */
public class CircularList<E> {

  private List<E> _elements;

  private int _currentPos = 0;

  /**
   * Constructs an empty list with the given initial capacity.
   * 
   * @param initialCapacity
   */
  public CircularList(int initialCapacity) {
    this(new ArrayList<E>(initialCapacity));
  }

  /**
   * Constructs an empty list with initial capacity of 10.
   */
  public CircularList() {
    this(10);
  }

  public CircularList(List<E> list) {
    _elements = list;
  }

  public CircularList(Collection<E> list) {
    _elements = new ArrayList<E>(list);
  }

  /**
   * Adds the element at top of this list
   * 
   * @param element
   */
  public void add(E element) {
    _elements.add(_currentPos, element);
  }

  /**
   * @return the top of this list
   */
  public E getTop() {
    if (size() == 0)
      return null;

    return _elements.get(_currentPos);
  }

  /**
   * @return the tail of this list
   */
  public E getTail() {
    if (size() == 0)
      return null;

    return _elements.get(getPreviousPos());
  }

  /**
   * Returns the top of this list and moves it to end.
   * 
   * @return the top of this list
   */
  public E getNext() {
    if (size() == 0)
      return null;

    E result = _elements.get(_currentPos);
    incrementPos();
    return result;
  }

  /**
   * Moves the top of this list to end.
   */
  public void moveToEnd() {
    incrementPos();
  }

  /**
   * Moves the top of this list to the middle.
   */
  public void moveToMid() {
    if (size() == 0)
      return;

    E element = _elements.remove(_currentPos);
    int midPos = size() / 2 + _currentPos;
    if (midPos > size())
      midPos -= size();
    _elements.add(midPos, element);
  }

  /**
   * @return the top of this list
   */
  public E removeTop() {
    if (size() == 0)
      return null;

    E top = _elements.remove(_currentPos);
    if (_currentPos == size())
      _currentPos = 0;
    return top;
  }

  /**
   * @param element
   * @return true if list contained this element
   */
  public boolean remove(E element) {
    boolean contained = _elements.remove(element);
    if (_currentPos == size())
      _currentPos = 0;
    return contained;
  }

  /**
   * @return the number of elements in this list
   * 
   */
  public int size() {
    return _elements.size();
  }

  public boolean isEmpty() {
    return _elements.isEmpty();
  }

  private void incrementPos() {
    _currentPos++;
    if (_currentPos == _elements.size())
      _currentPos = 0;
  }

  private int getPreviousPos() {
    if (_currentPos == 0) {
      return _elements.size() - 1;
    }
    return _currentPos - 1;
  }

  @Override
  public String toString() {
    return _elements.toString();
  }

  public List<E> asList() {
    return _elements;
  }

}
