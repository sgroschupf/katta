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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class IndexDuplicateReducerTest extends TestCase {

  public void testMerge() throws IOException {
    IndexDuplicateReducer reducer = new IndexDuplicateReducer();
    Set<DocumentInformation> hashSet = new HashSet<DocumentInformation>();
    final Text key = new Text("foo");

    final DocumentInformation collectedInformation = new DocumentInformation();
    collectedInformation.setSortValue("" + 10 * System.currentTimeMillis());

    for (int i = 0; i < 10; i++) {
      DocumentInformation information = new DocumentInformation();
      information.setDocId(i);
      if (i == 5) {
        information = collectedInformation;
      }
      hashSet.add(information);
    }

    Mockery mockery = new Mockery();
    final OutputCollector outputCollector = mockery.mock(OutputCollector.class);
    Reporter reporter = mockery.mock(Reporter.class);

    mockery.checking(new Expectations() {
      {
        one(outputCollector).collect(key, collectedInformation);
      }
    });

    reducer.reduce(key, hashSet.iterator(), outputCollector, reporter);

    mockery.assertIsSatisfied();
  }
}
