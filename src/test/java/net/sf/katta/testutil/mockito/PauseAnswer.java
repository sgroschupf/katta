/**
 * Copyright 2011 the original author or authors.
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

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class PauseAnswer<T> implements Answer<T> {

  private volatile boolean _executionStarted;
  private volatile boolean _pauseCanceled;
  private final T _returnValue;
  private Exception _resumeException;

  public PauseAnswer(T returnValue) {
    _returnValue = returnValue;
  }

  @Override
  public synchronized T answer(InvocationOnMock invocation) throws Throwable {
    notifyAll();// for joinExecutionBegin
    _executionStarted = true;
    if (!_pauseCanceled) {
      wait();
    }
    if (_resumeException != null) {
      throw _resumeException;
    }
    return _returnValue;
  }

  public synchronized void joinExecutionBegin() {
    try {
      if (!_executionStarted) {
        wait();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public synchronized void resumeExecution(boolean cancelAllFurtherPauses) {
    notifyAll();
    _executionStarted = false;
    if (cancelAllFurtherPauses) {
      _pauseCanceled = true;
    }
  }

  public void resumeExecutionWithException(boolean cancelAllFurtherPauses, Exception resumeException) {
    _resumeException = resumeException;
    resumeExecution(cancelAllFurtherPauses);
  }

}
