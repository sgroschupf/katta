package net.sf.katta.util;

public class StringUtil {

  public static String getCallingMethod(StackTraceElement[] stackTrace) {
    int indexOfCallingMethod = 2;
    if (stackTrace[0].toString().endsWith("(Native Method)")) {
      // on some jvm the native methods get printed on some not
      indexOfCallingMethod = 3;
    }
    return stackTrace[indexOfCallingMethod].toString();
  }
}
