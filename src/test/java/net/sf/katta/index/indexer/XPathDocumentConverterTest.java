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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class XPathDocumentConverterTest extends TestCase {

  public void testConvert() throws Exception {
    final IDocumentFactory<Text, Text> pathDocumentFactory = new XPathDocumentFactory();
    final JobConf jobConf = new JobConf();
    jobConf.set(XPathDocumentFactory.XPATH_INPUT_FILE, "src/test/resources/xpath.properties");
    pathDocumentFactory.configure(jobConf);

    final Text text = new Text("foo");
    final BufferedReader reader = new BufferedReader(new FileReader(new File("src/test/resources/user.xml")));
    String line = null;
    final StringBuilder stringBuilder = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      stringBuilder.append(line);
    }
    final Document document = pathDocumentFactory.convert(text, new Text(stringBuilder.toString()));
    Field field = document.getField("" + "/user/name/first".hashCode());
    assertEquals("foo", field.stringValue());
    field = document.getField("" + "/user/age".hashCode());
    assertEquals("0000000029", field.stringValue());
  }
}
