package net.sf.katta.protocol;

import java.io.Serializable;
import java.util.List;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

public class OperationQueue<T extends Serializable> {

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
  // private String _rootPath;
  private String _elementsPath;
  private String _resultsPath;

  public OperationQueue(ZkClient zkClient, String rootPath) {
    _zkClient = zkClient;
    _elementsPath = rootPath + "/operations";
    _resultsPath = rootPath + "/results";
    _zkClient.createPersistent(rootPath, true);
    _zkClient.createPersistent(_elementsPath, true);
    _zkClient.createPersistent(_resultsPath, true);
  }

  private String getElementRoughPath() {
    return getElementPath("operation" + "-");
  }

  private String getElementPath(String elementId) {
    return _elementsPath + "/" + elementId;
  }

  private String getResultPath(String elementId) {
    return _resultsPath + "/" + elementId;
  }

  /**
   * 
   * @param element
   * @return the id of the element in the queue
   */
  public String add(T element) {
    try {
      String sequential = _zkClient.createPersistentSequential(getElementRoughPath(), element);
      String elementId = sequential.substring(sequential.lastIndexOf('/') + 1);
      _zkClient.delete(getResultPath(elementId));
      return elementId;
    } catch (Exception e) {
      throw ExceptionUtil.convertToRuntimeException(e);
    }
  }

  public T remove() throws InterruptedException {
    return remove(null);
  }

  public T remove(Serializable result) throws InterruptedException {
    Element<T> element = getFirstElement();
    if (result != null) {
      _zkClient.createEphemeral(getResultPath(element.getName()), result);
    }
    _zkClient.delete(getElementPath(element.getName()));
    return element.getData();
  }

  public Serializable getResult(String elementId, boolean remove) {
    String zkPath = getResultPath(elementId);
    Serializable result = _zkClient.readData(zkPath, true);
    if (remove) {
      _zkClient.delete(zkPath);
    }
    return result;
  }

  public T peek() throws InterruptedException {
    Element<T> element = getFirstElement();
    if (element == null) {
      return null;
    }
    return element.getData();
  }

  public int size() {
    return _zkClient.getChildren(_elementsPath).size();
  }

  public boolean isEmpty() {
    return size() == 0;
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
        List<String> elementNames = _zkClient.subscribeChildChanges(_elementsPath, notifyListener);
        while (elementNames == null || elementNames.isEmpty()) {
          synchronized (mutex) {
            mutex.wait();
          }
          elementNames = _zkClient.getChildren(_elementsPath);
        }
        _zkClient.unsubscribeChildChanges(_elementsPath, notifyListener);
        String elementName = getSmallestElement(elementNames);
        try {
          String elementPath = getElementPath(elementName);
          return new Element<T>(elementName, (T) _zkClient.readData(elementPath));
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
