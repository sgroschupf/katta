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
package net.sf.katta.testutil.mockito;

import java.util.Arrays;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Useful to find out who called a given mock method.<br>
 * Usage:<br>
 * doAnswer(new PrintStacktraceAnswer()).when(mock).someMethod(notNull());
 * 
 */
public class PrintStacktraceAnswer implements Answer<Void> {

  @Override
  public Void answer(InvocationOnMock invocation) throws Throwable {
    System.out.println("called " + invocation.getMethod() + " with arguments "
            + Arrays.asList(invocation.getArguments()));
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    for (StackTraceElement string : stackTrace) {
      System.out.println("\t" + string.toString());
    }
    return null;
  }

}
