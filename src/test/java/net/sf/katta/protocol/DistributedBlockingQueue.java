package net.sf.katta.protocol;

import java.io.Serializable;
import java.util.List;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

public class DistributedBlockingQueue<T extends Serializable> {

  private static class Element<T> {
    private String _name;
    private T _data;

    public Element(String name, T data) {
      _name = name;
      _data = data;
    }

    public String getName() {
      return _name;
    }

    public T getData() {
      return _data;
    }
  }

  private ZkClient _zkClient;
  private String _root;

  private static final String ELEMENT_NAME = "element";

  public DistributedBlockingQueue(ZkClient zkClient, String root) {
    _zkClient = zkClient;
    _root = root;
  }

  /**
   * 
   * @param element
   * @return the id of the element in the queue
   */
  public String offer(T element) {
    try {
      String sequential = _zkClient.createPersistentSequential(_root + "/" + ELEMENT_NAME + "-", element);
      return sequential.substring(sequential.lastIndexOf('/') + 1);
    } catch (Exception e) {
      throw ExceptionUtil.convertToRuntimeException(e);
    }
  }

  public T poll() throws InterruptedException {
    while (true) {
      Element<T> element = getFirstElement();
      if (element == null) {
        return null;
      }

      try {
        _zkClient.delete(element.getName());
        return element.getData();
      } catch (ZkNoNodeException e) {
        // somebody else picked up the element first, so we have to
        // retry with the new first element
      } catch (Exception e) {
        throw ExceptionUtil.convertToRuntimeException(e);
      }
    }
  }

  private String getSmallestElement(List<String> list) {
    String smallestElement = list.get(0);
    for (String element : list) {
      if (element.compareTo(smallestElement) < 0) {
        smallestElement = element;
      }
    }

    return smallestElement;
  }

  public T peek() throws InterruptedException {
    Element<T> element = getFirstElement();
    if (element == null) {
      return null;
    }
    return element.getData();
  }

  public int size() {
    return _zkClient.getChildren(_root).size();
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  @SuppressWarnings("unchecked")
  private Element<T> getFirstElement() throws InterruptedException {
    try {
      while (true) {
        final Object mutex = new Object();
        IZkChildListener notifyListener = new IZkChildListener() {
          @Override
          public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized (mutex) {
              mutex.notify();
            }
          }
        };
        List<String> elementNames = _zkClient.subscribeChildChanges(_root, notifyListener);
        while (elementNames == null || elementNames.isEmpty()) {
          synchronized (mutex) {
            mutex.wait();
          }
          elementNames = _zkClient.getChildren(_root);
        }
        _zkClient.unsubscribeChildChanges(_root, notifyListener);
        String elementName = getSmallestElement(elementNames);
        try {
          return new Element<T>(_root + "/" + elementName, (T) _zkClient.readData(_root + "/" + elementName));
        } catch (ZkNoNodeException e) {
          // somebody else picked up the element first, so we have to
          // retry with the new first element
        }
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw ExceptionUtil.convertToRuntimeException(e);
    }
  }

}
