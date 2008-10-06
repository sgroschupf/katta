package net.sf.katta.testutil;

import java.util.ArrayList;
import java.util.List;

import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;

/**
 * A jmock2 {@link Action} which store the parameter a invocation.
 */
public class StoreAction extends CustomAction {

  private List<Object[]> _parameters = new ArrayList<Object[]>();

  private Action _actionToInvoke;

  public StoreAction() {
    super("this action will store the parameters of all method calls");
  }

  public StoreAction(Action actionToInvoke) {
    this();
    _actionToInvoke = actionToInvoke;
  }

  public Object invoke(Invocation invocation) throws Throwable {
    Object[] parametersAsArray = invocation.getParametersAsArray();
    _parameters.add(parametersAsArray);
    if (_actionToInvoke != null) {
      return _actionToInvoke.invoke(invocation);
    }
    return null;
  }

  public List<Object[]> getParameters() {
    return _parameters;
  }

  public void reset() {
    _parameters.clear();
  }

}
