/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.IndexReader;

public class DfsIndexRecordReader implements RecordReader<Text, DocumentInformation> {

  private final static Logger LOG = Logger.getLogger(DfsIndexRecordReader.class);

  private IDocumentDuplicateInformation _duplicateInformation;

  private IndexReader _indexReader;

  private int _maxDoc;

  private int _doc;

  private Path _indexPath;

  public static final String INVALID = "INVALID";

  private FileSplit _fileSplit;

  public DfsIndexRecordReader(JobConf jobConf, InputSplit inputSplit, IDocumentDuplicateInformation duplicateInformation)
      throws IOException {
    _duplicateInformation = duplicateInformation;
    FileSystem fileSystem = FileSystem.get(jobConf);
    _fileSplit = (FileSplit) inputSplit;
    Path indexPath = _fileSplit.getPath();
    // we use md5 for uncompressed folder, because some shards can have the same
    // name
    String md5 = MD5Hash.digest(indexPath.toString()).toString();
    Path workingFolder = new Path(jobConf.getOutputPath(), ".indexes/" + indexPath.getName() + "-" + md5
        + "-uncompress");
    // the outputpath is modified by hadoop and will be extend with
    // "_temporary/jobId"
    _indexPath = new Path(jobConf.getOutputPath().getParent().getParent(), ".indexes/" + indexPath.getName() + "-"
        + md5 + "-uncompress");
    try {
      _indexReader = IndexReader.open(new DfsIndexDirectory(fileSystem, indexPath, workingFolder));
      _maxDoc = _indexReader.maxDoc();
    } catch (Exception e) {
      LOG.warn("can not open index '" + indexPath + "', ignore this index.", e);
    }
  }

  public boolean next(Text key, DocumentInformation value) throws IOException {
    boolean ret = false;
    if (_doc < _maxDoc) {
      ret = true;
      String keyInfo = null;
      String sortValue = null;

      try {
        MapFieldSelector selector = new MapFieldSelector(_duplicateInformation.getSupportedFieldNames());
        Document document = _indexReader.document(_doc, selector);
        keyInfo = _duplicateInformation.getKey(document);
        sortValue = _duplicateInformation.getSortValue(document);
      } catch (Exception e) {
        LOG.warn("can not read document '" + _doc + "' from split '" + _fileSplit.getPath() + "'", e);
      }

      if ((keyInfo == null || keyInfo.trim().equals(""))) {
        keyInfo = INVALID;
      }

      if ((sortValue == null || sortValue.trim().equals(""))) {
        sortValue = "" + Integer.MIN_VALUE;
      }

      key.set(keyInfo);
      value.setDocId(_doc);
      value.setSortValue(sortValue);
      value.setIndexPath(_indexPath.toString());
      _doc++;
    }

    return ret;
  }

  public Text createKey() {
    return new Text();
  }

  public DocumentInformation createValue() {
    return new DocumentInformation();
  }

  public long getPos() throws IOException {
    return _doc;
  }

  public void close() throws IOException {
    if (_indexReader != null) {
      _indexReader.close();
    }
  }

  public float getProgress() throws IOException {
    return (float) _doc / _maxDoc;
  }

}
