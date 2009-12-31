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
package net.sf.katta;

import net.sf.katta.testutil.PrintMethodNames;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class AbstractTest {

  @Rule
  public TemporaryFolder _temporaryFolder = new TemporaryFolder();

  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  protected Matcher<Long> almostEquals(final long value1, final long aberration) {
    return new BaseMatcher<Long>() {
      @Override
      public boolean matches(Object value2) {
        Long long2 = (Long) value2;
        return Math.abs(value1 - long2) < aberration;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" equals " + value1 + " with aberration of " + aberration);
      }
    };
  }
}
