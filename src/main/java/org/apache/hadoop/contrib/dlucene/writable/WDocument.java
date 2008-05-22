/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.contrib.dlucene.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults.FieldType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

/**
 * A {@link Writable} wrapper for {@link Document}.
 */
public class WDocument implements Writable {

  /** The document. */
  private Document document = null;

  /**
   * Constructor.
   * 
   * @param document the Document.
   */
  public WDocument(Document document) {
    this.document = document;
  }

  /**
   * Constructor.
   */
  private WDocument() {
    this.document = new Document();
  }

  /**
   * Get the document being wrapped.
   * 
   * @return the Document
   */
  public Document getDocument() {
    return this.document;
  }

  /**
   * Deserialize the document.
   * 
   * @param in the input stream
   * @return the document
   * @throws IOException
   */
  public static WDocument read(DataInput in) throws IOException {
    WDocument document = new WDocument();
    document.readFields(in);
    return document;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    Utils.checkArgs(out);
    List<Field> fields = document.getFields();
    out.writeInt(fields.size());
    for (Field f : fields) {
      out.writeBoolean(f.isCompressed());
      out.writeBoolean(f.isIndexed());
      out.writeBoolean(f.isStored());
      out.writeBoolean(f.isTokenized());
      out.writeBoolean(f.getOmitNorms());

      // out.writeBoolean(f.isBinary());
      // out.writeBoolean(f.isLazy());
      // out.writeBoolean(f.isStoreOffsetWithTermVector());
      // out.writeBoolean(f.isStorePositionWithTermVector());
      // out.writeBoolean(f.isTermVectorStored());
      // out.writeFloat(f.getBoost());

      Text.writeString(out, f.name());

      FieldType fType = null;
      if (f.readerValue() != null)
        fType = FieldType.READER;
      else if (f.stringValue() != null)
        fType = FieldType.STRING;
      else if (f.binaryValue() != null)
        fType = FieldType.BINARY;
      else if (f.tokenStreamValue() != null)
        fType = FieldType.TOKENSTREAM;
      if (fType != null) {
        out.writeInt(fType.ordinal());
      }

      if (fType == FieldType.STRING)
        Text.writeString(out, f.stringValue());
      else if (fType == FieldType.BINARY) {
        out.writeInt(f.binaryValue().length);
        out.write(f.binaryValue());
      } else if (fType == FieldType.READER)
        throw new IOException(
            "Fields of type Reader are not currently supported");
      else if (fType == FieldType.TOKENSTREAM)
        throw new IOException(
            "Fields of type TokenStream are not currently supported");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);

    int m = in.readInt();
    for (int j = 0; j < m; j++) {

      boolean isCompressed = in.readBoolean();
      boolean isIndexed = in.readBoolean();
      boolean isStored = in.readBoolean();
      boolean isTokenized = in.readBoolean();
      boolean omitNorms = in.readBoolean();

      // boolean isBinary = in.readBoolean();
      // boolean isLazy = in.readBoolean();
      // boolean isStoreOffsetWithTermVector = in.readBoolean();
      // boolean isStorePositionWithTermVector = in.readBoolean();
      // boolean isTermVectorStored = in.readBoolean();
      // float boost = in.readFloat();

      String name = Text.readString(in);
      Field.Store store = null;
      if (isStored && !isCompressed) {
        store = Store.YES;
      } else if (isStored && isCompressed) {
        store = Store.COMPRESS;
      } else if (!isStored && !isCompressed) {
        store = Store.NO;
      }

      Field.Index index = null;
      if (!isIndexed && !isTokenized) {
        index = Index.NO;
      } else if (isIndexed && isTokenized) {
        index = Index.TOKENIZED;
      } else if (isIndexed && !isTokenized && !omitNorms) {
        index = Index.UN_TOKENIZED;
      } else if (isIndexed && !isTokenized && omitNorms) {
        index = Index.NO_NORMS;
      }

      FieldType fType = FieldType.values()[in.readInt()];
      Field f = null;
      String sValue = null;
      byte[] bValue = null;
      if (fType == FieldType.STRING) {
        sValue = Text.readString(in);
        f = new Field(name, sValue, store, index);
      } else if (fType == FieldType.BINARY) {
        int length = in.readInt();
        bValue = new byte[length];
        in.readFully(bValue);
        f = new Field(name, bValue, store);
      } else if (fType == FieldType.READER)
        throw new IOException(
            "Fields of type Reader are not currently supported");
      else if (fType == FieldType.TOKENSTREAM)
        throw new IOException(
            "Fields of type TokenStream are not currently supported");

      document.add(f);
    }
  }
}
