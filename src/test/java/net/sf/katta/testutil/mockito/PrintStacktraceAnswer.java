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
