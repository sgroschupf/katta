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
package net.sf.katta.node;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

public class QueryWritableTest extends TestCase {

  public void testSerializeQuery() throws Exception {
    TermQuery termQuery = new TermQuery(new Term("katta"));
    QueryWritable writable = new QueryWritable(termQuery);

    DataOutputBuffer out = new DataOutputBuffer(1024);
    writable.write(out);
    out.flush();

    DataInputBuffer inputBuffer = new DataInputBuffer();
    inputBuffer.reset(out.getData(), out.getData().length);
    QueryWritable writable2 = new QueryWritable();
    writable2.readFields(inputBuffer);

    Assert.assertTrue(writable.equals(writable2));
  }

}
