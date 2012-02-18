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
import java.util.Iterator;
import java.util.List;

/**
 * @author Peter Voss
 */
public class MergeSort {

  public static void main(final String[] args) {
    // int pos = 0;
    // while (true) {
    // List<Hit> tmp = new ArrayList<Hit>(_hitsList.size());
    // for (List<Hit> hitList : _hitsList) {
    // if (hitList.size() > pos) {
    // tmp.add(hitList.get(pos));
    // }
    // }
    // if (tmp.size() == 0) {
    // // we are done no new data
    // return;
    // }
    // Collections.sort(tmp);
    // _sortedList.addAll(tmp);
    // pos++;
    // }

    // System.out.println(mergeSort(Arrays.asList(1, 2, 3, 4, 5, 6)));
    // System.out.println(mergeSort(Arrays.asList(6, 5, 4, 3, 2)));
    // System.out.println(mergeSort(Arrays.asList(3, 1, 10, -6, 17, 18,
    // 19)));
  }

  public static <T extends Comparable<T>> List<T> merge(final List<T>... sortedLists) {
    final List<Iterator<T>> iterators = new ArrayList<Iterator<T>>();

    for (final List<T> list : sortedLists) {
      if (!list.isEmpty()) {
        iterators.add(list.iterator());
      }
    }

    final List<T> currentElements = new ArrayList<T>();
    for (final Iterator<T> iterator : iterators) {
      currentElements.add(iterator.next());
    }

    final List<T> sortedResult = new ArrayList<T>();
    while (iterators.size() > 0) {
      int listWithSmallesElement = 0;
      for (int i = 1; i < currentElements.size(); i++) {
        if (currentElements.get(listWithSmallesElement).compareTo(currentElements.get(i)) > 0) {
          listWithSmallesElement = i;
        }
      }

      sortedResult.add(currentElements.get(listWithSmallesElement));

      if (iterators.get(listWithSmallesElement).hasNext()) {
        currentElements.set(listWithSmallesElement, iterators.get(listWithSmallesElement).next());
      } else {
        // we remove this list
        iterators.remove(listWithSmallesElement);
        currentElements.remove(listWithSmallesElement);
      }
    }
    return sortedResult;
  }

  public static <T extends Comparable<T>> List<T> mergerSG(final List<List<T>> unsortedLists) {

    List<T> leftList = unsortedLists.get(0);
    for (int i = 1; i < unsortedLists.size(); i++) {
      final List<T> rightList = unsortedLists.get(i);
      final Iterator<T> leftIterator = leftList.iterator();
      final Iterator<T> rightIterator = rightList.iterator();

      T currentLeftElement = leftIterator.next();
      T currentRightElement = rightIterator.next();

      final List<T> sortedResult = new ArrayList<T>();
      while (true) {
        final int comparison = (currentLeftElement.compareTo(currentRightElement));
        if (comparison <= 0) {
          // the left one comes first
          sortedResult.add(currentLeftElement);
          currentLeftElement = null;
        } else {
          // the right one comes first
          sortedResult.add(currentRightElement);
          currentRightElement = null;
        }

        if (currentLeftElement == null) {
          // go to next element
          if (leftIterator.hasNext()) {
            currentLeftElement = leftIterator.next();
          } else {
            // we can add all element from the right half and
            // quit
            if (currentRightElement != null) {
              sortedResult.add(currentRightElement);
            }
            while (rightIterator.hasNext()) {
              sortedResult.add(rightIterator.next());
            }
            break;
            // return sortedResult;
          }
        }
        if (currentRightElement == null) {
          // go to next element
          if (rightIterator.hasNext()) {
            currentRightElement = rightIterator.next();
          } else {
            // we can add all element from the left half and
            // quit
            if (currentLeftElement != null) {
              sortedResult.add(currentLeftElement);
            }
            while (leftIterator.hasNext()) {
              sortedResult.add(leftIterator.next());
            }
            break;
            // return sortedResult;
          }
        }

      }
      leftList = sortedResult;
    }
    return leftList;
  }

  // only max 1 element -> we are sorted already
  // if (unsortedList.size() < 2) {
  // return unsortedList;
  // }

  // devide into 2 sub lists
  // int middle = unsortedList.size() / 2;
  // List<T> leftHalf = unsortedList.subList(0, middle);
  // List<T> rightHalf = unsortedList.subList(middle, unsortedList.size());

  // sort both lists:
  // List<T> sortedLeftHalf = mergeSort(leftHalf);
  // List<T> sortedRightHalf = mergeSort(rightHalf);

  // merge two sorted lists
  // Iterator<T> leftIterator = sortedLeftHalf.iterator();
  // Iterator<T> rightIterator = sortedRightHalf.iterator();

  // T currentLeftElement = leftIterator.next();
  // T currentRightElement = rightIterator.next();
  //
  // List<T> sortedResult = new ArrayList<T>();
  // while (true) {
  // int comparison = (currentLeftElement.compareTo(currentRightElement));
  // if (comparison == 0) {
  // // equal, so we first add the left one then the right
  // sortedResult.add(currentLeftElement);
  // sortedResult.add(currentRightElement);
  //
  // currentLeftElement = null;
  // currentRightElement = null;
  // } else if (comparison < 0) {
  // // the left one comes first
  // sortedResult.add(currentLeftElement);
  //
  // currentLeftElement = null;
  // } else {
  // // the right one comes first
  // sortedResult.add(currentRightElement);
  //
  // currentRightElement = null;
  // }

  // if (currentLeftElement == null) {
  // // go to next element
  // if (leftIterator.hasNext()) {
  // currentLeftElement = leftIterator.next();
  // } else {
  // // we can add all element from the right half and quit
  // if (currentRightElement != null) {
  // sortedResult.add(currentRightElement);
  // }
  // while (rightIterator.hasNext()) {
  // sortedResult.add(rightIterator.next());
  // }
  // return sortedResult;
  // }
  // }
  // if (currentRightElement == null) {
  // // go to next element
  // if (rightIterator.hasNext()) {
  // currentRightElement = rightIterator.next();
  // } else {
  // // we can add all element from the left half and quit
  // if (currentLeftElement != null) {
  // sortedResult.add(currentLeftElement);
  // }
  // while (leftIterator.hasNext()) {
  // sortedResult.add(leftIterator.next());
  // }
  // return sortedResult;
  // }
  // }
  // }
  // }
}
