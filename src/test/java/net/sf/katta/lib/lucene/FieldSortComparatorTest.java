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
package net.sf.katta.lib.lucene;

import static org.junit.Assert.assertEquals;
import net.sf.katta.AbstractTest;
import net.sf.katta.util.WritableType;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.lucene.search.SortField;
import org.junit.Test;

public class FieldSortComparatorTest extends AbstractTest {

  @Test
  public void testSingleIntFieldCompare() {
    SortField[] sortFields = new SortField[] { new SortField("intField", SortField.INT) };
    WritableType[] sortFieldTypes = new WritableType[] { WritableType.INT };
    FieldSortComparator fieldSortComparator = new FieldSortComparator(sortFields, sortFieldTypes);

    Hit hit1 = new Hit("shard", "node", 0.0f, 1, sortFieldTypes);
    Hit hit2 = new Hit("shard", "node", 0.0f, 2, sortFieldTypes);

    hit1.setSortFields(new WritableComparable[] { new IntWritable(1) });
    hit2.setSortFields(new WritableComparable[] { new IntWritable(2) });

    assertEquals(0, fieldSortComparator.compare(hit1, hit1));
    assertEquals(1, fieldSortComparator.compare(hit2, hit1));
    assertEquals(-1, fieldSortComparator.compare(hit1, hit2));
  }

  @Test
  public void testTwoIntFieldCompare() {
    SortField[] sortFields = new SortField[] { new SortField("intField1", SortField.INT),
            new SortField("intField2", SortField.INT) };
    WritableType[] sortFieldTypes = new WritableType[] { WritableType.INT, WritableType.INT };
    FieldSortComparator fieldSortComparator = new FieldSortComparator(sortFields, sortFieldTypes);

    Hit hit1 = new Hit("shard", "node", 0.0f, 1, sortFieldTypes);
    Hit hit2 = new Hit("shard", "node", 0.0f, 2, sortFieldTypes);

    hit1.setSortFields(new WritableComparable[] { new IntWritable(1), new IntWritable(1) });
    hit2.setSortFields(new WritableComparable[] { new IntWritable(1), new IntWritable(2) });

    assertEquals(0, fieldSortComparator.compare(hit1, hit1));
    assertEquals(1, fieldSortComparator.compare(hit2, hit1));
    assertEquals(-1, fieldSortComparator.compare(hit1, hit2));
  }

  @Test
  public void testStringFieldDocIdCompare() {
    SortField[] sortFields = new SortField[] { new SortField("stringField", SortField.STRING),
            new SortField("docIdField", SortField.DOC) };
    WritableType[] sortFieldTypes = new WritableType[] { WritableType.TEXT, WritableType.INT };
    FieldSortComparator fieldSortComparator = new FieldSortComparator(sortFields, sortFieldTypes);

    Hit hit1 = new Hit("shard", "node", 0.0f, 1, sortFieldTypes);
    Hit hit2 = new Hit("shard", "node", 0.0f, 2, sortFieldTypes);

    hit1.setSortFields(new WritableComparable[] { new Text("a"), new IntWritable(1) });
    hit2.setSortFields(new WritableComparable[] { new Text("a"), new IntWritable(2) });

    assertEquals(0, fieldSortComparator.compare(hit1, hit1));
    assertEquals(1, fieldSortComparator.compare(hit2, hit1));
    assertEquals(-1, fieldSortComparator.compare(hit1, hit2));
  }

  @Test
  public void testStringFieldScoreCompare() {
    SortField[] sortFields = new SortField[] { new SortField("stringField", SortField.STRING),
            new SortField("scoreField", SortField.SCORE) };
    WritableType[] sortFieldTypes = new WritableType[] { WritableType.TEXT, WritableType.FLOAT };
    FieldSortComparator fieldSortComparator = new FieldSortComparator(sortFields, sortFieldTypes);

    Hit hit1 = new Hit("shard", "node", 0.5f, 1, sortFieldTypes);
    Hit hit2 = new Hit("shard", "node", 0.2f, 2, sortFieldTypes);

    hit1.setSortFields(new WritableComparable[] { new Text("a"), new FloatWritable(hit1.getScore()) });
    hit2.setSortFields(new WritableComparable[] { new Text("a"), new FloatWritable(hit2.getScore()) });

    assertEquals(0, fieldSortComparator.compare(hit1, hit1));
    assertEquals(1, fieldSortComparator.compare(hit2, hit1));
    assertEquals(-1, fieldSortComparator.compare(hit1, hit2));
  }

}
