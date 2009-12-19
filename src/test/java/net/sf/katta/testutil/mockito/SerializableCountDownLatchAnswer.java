package net.sf.katta.testutil.mockito;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings("serial")
public class SerializableCountDownLatchAnswer implements Answer<Void>, Serializable {

  private static CountDownLatch _countDownLatch;

  public SerializableCountDownLatchAnswer(int latchCount) {
    _countDownLatch = new CountDownLatch(latchCount);
  }

  @Override
  public Void answer(InvocationOnMock invocation) throws Throwable {
    _countDownLatch.countDown();
    return null;
  }

  public CountDownLatch getCountDownLatch() {
    return _countDownLatch;
  }
}
