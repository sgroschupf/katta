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
package net.sf.katta.util;

import org.junit.Test;

import static org.junit.Assert.fail;

import static org.fest.assertions.Assertions.assertThat;

public class ClassUtilTest {

  @Test
  public void testPrivateField() throws Exception {
    TestClass1 testClass1 = new TestClass1();
    TestClass2 testClass2 = new TestClass2();

    assertThat(ClassUtil.getPrivateFieldValue(testClass1, "field1")).isEqualTo("1");
    assertThat(ClassUtil.getPrivateFieldValue(testClass2, "field2")).isEqualTo("2");
    assertThat(ClassUtil.getPrivateFieldValue(testClass2, "field1")).isEqualTo("1");

    try {
      ClassUtil.getPrivateFieldValue(testClass2, "fieldXY");
      fail("should throw exception");
    } catch (Exception e) {
      // expected
    }
  }

  private static class TestClass1 {
    @SuppressWarnings("unused")
    private String field1 = "1";
  }

  private static class TestClass2 extends TestClass1 {
    @SuppressWarnings("unused")
    private String field2 = "2";
  }
}
