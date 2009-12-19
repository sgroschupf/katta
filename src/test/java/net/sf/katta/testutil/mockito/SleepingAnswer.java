package net.sf.katta.testutil.mockito;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SleepingAnswer implements Answer<Void> {

  private final long _timeToSleep;

  public SleepingAnswer() {
    this(Long.MAX_VALUE);
  }

  public SleepingAnswer(long timeToSleep) {
    _timeToSleep = timeToSleep;
  }

  @Override
  public Void answer(InvocationOnMock invocation) throws Throwable {
    Thread.sleep(_timeToSleep);
    return null;
  }

}
