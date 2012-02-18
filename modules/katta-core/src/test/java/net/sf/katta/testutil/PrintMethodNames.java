/**
 * Copyright 2009 the original author or authors.
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

import org.apache.log4j.Logger;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class PrintMethodNames implements MethodRule {

  protected static final Logger LOG = Logger.getLogger(PrintMethodNames.class);

  private String _methodName;

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod method, Object target) {
    _methodName = method.getName();
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        String testClassName = method.getMethod().getDeclaringClass().getName();
        LOG.info("~~~~~~~~~~~~~~~ " + testClassName + "#" + method.getName() + "() [" + Thread.activeCount() + "] ~");
        statement.evaluate();
        LOG
                .info("~~~~~~~~~~~~~~~ FIN " + testClassName + "#" + method.getName() + "() [" + Thread.activeCount()
                        + "]~");
      }
    };
  }

  public String getCurrentMethodName() {
    return _methodName;
  }
}
