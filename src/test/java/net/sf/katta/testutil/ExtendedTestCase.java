package net.sf.katta.testutil;

import java.lang.reflect.Method;

import junit.framework.TestCase;

/**
 * JUnit 3 test case with JUnit 4 like {@link #beforeClass()} and
 * {@link #afterClass()} methods.
 * 
 * Inspired by
 * <i>http://blog.jakubpawlowicz.com/2006/08/31/beforeclass-and-afterclass
 * -with-junit-3</i>
 * 
 */
public abstract class ExtendedTestCase extends TestCase {

  private static int _testMethodsLeft = 0;

  private int countTestMethods() {
    int testMethodCount = 0;
    for (Method method : getClass().getMethods()) {
      if (method.getName().startsWith("test")) {
        testMethodCount++;
      }
    }
    return testMethodCount;
  }

  @Override
  protected final void setUp() throws Exception {
    if (_testMethodsLeft == 0) {
      _testMethodsLeft = countTestMethods();
      System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + " ~~~~~~~~~~~~~~~");
      beforeClass();
    }
    System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + "#" + getName() + "() ~~~~~~~~~~~~~~~");
    onSetUp();
  }

  @Override
  protected final void tearDown() throws Exception {
    onTearDown();
    if (--_testMethodsLeft == 0) {
      System.out.println("~~~~~~~~~~~~~~~ FINISHING " + getClass().getName() + " ~~~~~~~~~~~~~~~");
      afterClass();
    }
  }

  protected void onTearDown() throws Exception {
    // subclasses may override
  }

  protected void onSetUp() throws Exception {
    // subclasses may override
  }

  protected void beforeClass() throws Exception {
    // subclasses may override
  }

  protected void afterClass() throws Exception {
    // subclasses may override
  }
}
