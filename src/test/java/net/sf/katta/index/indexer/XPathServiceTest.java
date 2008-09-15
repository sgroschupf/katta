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
package net.sf.katta.index.indexer;

import junit.framework.TestCase;

public class XPathServiceTest extends TestCase {

  public void testIndex() throws Exception {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("<user>");
    stringBuilder.append("<name>");
    stringBuilder.append("<first>");
    stringBuilder.append("foo");
    stringBuilder.append("</first>");
    stringBuilder.append("<last>");
    stringBuilder.append("bar");
    stringBuilder.append("</last>");
    stringBuilder.append("</name>");
    stringBuilder.append("<login>");
    stringBuilder.append("foobar");
    stringBuilder.append("</login>");
    stringBuilder.append("<password>");
    stringBuilder.append("pwd");
    stringBuilder.append("</password>");
    stringBuilder.append("</user>");
    final String xml = stringBuilder.toString();
    final IXPathService service = new DefaultXPathService();
    String parse = service.parse("/user/name/first", xml);
    assertEquals("foo", parse);
    parse = service.parse("/user/name/last", xml);
    assertEquals("bar", parse);
  }
}
