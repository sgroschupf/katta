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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

public class SortWritable implements Writable {

  private Sort _sort;

  public SortWritable() {
    // default constructor
  }

  public SortWritable(Sort sort) {
    _sort = sort;

  }

  @Override
  public void readFields(DataInput input) throws IOException {
    // TODO verify that this doesn't miss anything important & document limitations
    int numRecords = input.readInt();
    SortField[] sortFields = new SortField[numRecords];
    for (int i = 0; i < numRecords; i++) {
      String field = input.readUTF();
      SortField.Type type = SortField.Type.valueOf(input.readUTF());
      boolean reverse = input.readBoolean();
      sortFields[i] = new SortField(field, type, reverse);
    }

    _sort = new Sort(sortFields);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    SortField[] sortFields = _sort.getSort();
    output.writeInt(sortFields.length);
    for (SortField sortField : sortFields) {
      output.writeUTF(sortField.getField());
      output.writeUTF(sortField.getType().name());
      output.writeBoolean(sortField.getReverse());
    }
  }

  public Sort getSort() {
    return _sort;
  }

  @Override
  public boolean equals(Object obj) {
    Sort other = ((SortWritable) obj).getSort();
    return _sort.equals(other);
  }
}
