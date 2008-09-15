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
