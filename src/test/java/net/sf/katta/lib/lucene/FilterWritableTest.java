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
package net.sf.katta.lib.lucene;

import net.sf.katta.AbstractWritableTest;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FilterWritableTest extends AbstractWritableTest {

  @Test
  public void testSerializeFilter() throws Exception {
    TermQuery termQuery = new TermQuery(new Term("katta"));
    QueryWrapperFilter filter = new QueryWrapperFilter(termQuery);
    FilterWritable writable = new FilterWritable(filter);
    DataOutputBuffer buffer = writeWritable(writable);

    FilterWritable writable2 = new FilterWritable();
    readWritable(buffer, writable2);

    assertTrue(writable.equals(writable2));
  }

}
