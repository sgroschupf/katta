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
package net.sf.katta.index.indexer;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

public class Indexer implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {

  protected final static Logger LOG = Logger.getLogger(Indexer.class);

  public static enum DocumentCounter {
    DOCUMENT_COUNT, INDEXED_DOCUMENT_COUNT
  }

  private IDocumentFactory<WritableComparable, Writable> _factory;

  private String _tmpIndexDirectory;

  private IIndexPublisher _indexPublisher;

  private int _indexFlushThreshold;

  private int _indexerMaxMerge;

  private int _indexerMergeFactor;

  private int _termIndexIntervall;

  private int _maxFieldLength;

  private int _maxBufferedDocs;

  private IZipService _zipService;

  private WritableComparable _inputKey;

  private Writable _inputValue;

  final DataInputBuffer _inputBuffer = new DataInputBuffer();

  public void configure(final JobConf jobConf) {
    _factory = getDocumentFactory(jobConf);
    _indexPublisher = getPublisher(jobConf);
    _zipService = getZipper(jobConf);
    _tmpIndexDirectory = jobConf.get(IndexJobConf.INDEX_TMP_DIRECTORY, (System.getProperty("java.io.tmpdir")))
        + File.separator + System.currentTimeMillis();
    _indexFlushThreshold = jobConf.getInt(IndexJobConf.FLUSH_THRESHOLD, 10);
    _indexerMaxMerge = jobConf.getInt(IndexJobConf.INDEXER_MAX_MERGE, 10);
    _indexerMergeFactor = jobConf.getInt(IndexJobConf.INDEXER_MERGE_FACTOR, 10);
    _termIndexIntervall = jobConf.getInt(IndexJobConf.INDEXER_TERM_INTERVALL, 128);
    _maxFieldLength = jobConf.getInt(IndexJobConf.INDEXER_MAX_FIELD_LENGTH, 10000);
    _maxBufferedDocs = jobConf.getInt(IndexJobConf.INDEXER_MAX_BUFFERED_DOCS, 10);
    final Class<?> inputKeyClass = jobConf.getClass(IndexJobConf.INPUT_KEY_CLASS, WritableComparable.class);
    final Class<?> inputValueClass = jobConf.getClass(IndexJobConf.INPUT_VALUE_CLASS, Writable.class);
    try {
      _inputKey = (WritableComparable) inputKeyClass.newInstance();
      _inputValue = (Writable) inputValueClass.newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("can not instantiate input key '" + inputKeyClass.getName() + "' and input value '"
          + inputValueClass.getName() + "'class: ", e);
    }

  }

  public void reduce(final WritableComparable key, final Iterator<Writable> values,
      final OutputCollector<WritableComparable, Writable> collector, final Reporter reporter) throws IOException {
    final File indexDirectory = new File(_tmpIndexDirectory, key.toString());
    final FSDirectory directory = FSDirectory.getDirectory(indexDirectory);
    final IndexWriter indexWriter = new IndexWriter(directory, _factory.getIndexAnalyzer());
    indexWriter.setMaxMergeDocs(_indexerMaxMerge);
    indexWriter.setMergeFactor(_indexerMergeFactor);
    indexWriter.setTermIndexInterval(_termIndexIntervall);
    indexWriter.setMaxBufferedDocs(_maxBufferedDocs);
    indexWriter.setMaxFieldLength(_maxFieldLength);

    reporter.setStatus("indexing documents...");

    int counter = 0;
    while (values.hasNext()) {
      final Writable value = values.next();
      final BytesWritable bytesWritable = (BytesWritable) value;

      final byte[] bytes = bytesWritable.get();
      _inputBuffer.reset(bytes, bytes.length);
      _inputKey.readFields(_inputBuffer);
      _inputValue.readFields(_inputBuffer);

      final Document document = _factory.convert(_inputKey, _inputValue);
      if (document != null) {
        indexWriter.addDocument(document);
        counter++;
        reporter.incrCounter(DocumentCounter.INDEXED_DOCUMENT_COUNT, 1);
      } else {
        LOG.warn(_factory.getClass().getName() + " can not create document");
      }
      reporter.incrCounter(DocumentCounter.DOCUMENT_COUNT, 1);
      if (counter % _indexFlushThreshold == 0) {
        indexWriter.flush();
      }
    }
    LOG.info(counter + " documents are added to index.");

    // optimize
    Thread thread = createStatusThread(reporter, "Optimize Index...");
    indexWriter.optimize();
    indexWriter.close();
    thread.interrupt();

    // zip
    thread = createStatusThread(reporter, "Zip Index...");
    final boolean success = _zipService.zipFolder(indexDirectory, new File(indexDirectory + ".zip"));
    thread.interrupt();

    if (success) {
      // upload
      thread = createStatusThread(reporter, "Publish Index...");
      _indexPublisher.publish(indexDirectory.getAbsolutePath() + ".zip");
      FileUtil.fullyDelete(new File(indexDirectory + ".zip"));
      thread.interrupt();
    }
    FileUtil.fullyDelete(indexDirectory.getParentFile());

    // done
    reporter.setStatus("Indexing done. " + counter + " Documents added to index.");
  }

  public void close() throws IOException {
    // nothing to do
  }

  private Thread createStatusThread(final Reporter reporter, final String status) {
    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          while (true) {
            reporter.setStatus(status);
            sleep(1000);
          }
        } catch (final InterruptedException e) {
          LOG.info("status thread is stopped: " + status);
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private IZipService getZipper(final JobConf jobConf) {
    IZipService zipService = new ZipService();
    final String className = jobConf.get(IndexJobConf.INDEX_ZIP_CLASS);
    if (className != null) {
      try {
        final Class<IZipService> clazz = (Class<IZipService>) Class.forName(className);
        zipService = clazz.newInstance();
      } catch (final Exception e) {
        throw new RuntimeException("can not create zipper, because it is not available.", e);
      }
    }
    return zipService;
  }

  private IIndexPublisher getPublisher(final JobConf configuration) {
    IIndexPublisher distributer = new IndexUploader();
    final String indexdistributerClass = configuration.get(IndexJobConf.INDEX_PUBLISHER_CLASS);
    if (indexdistributerClass != null) {
      try {
        final Class<IIndexPublisher> clazz = (Class<IIndexPublisher>) Class.forName(indexdistributerClass);
        distributer = clazz.newInstance();
      } catch (final Exception e) {
        throw new RuntimeException("can not create distributer, because it is not available.", e);
      }
    }
    try {
      distributer.configure(configuration);
    } catch (final Exception e) {
      throw new RuntimeException("exception while configure the index publisher '" + distributer.getClass().getName()
          + "'", e);
    }
    return distributer;
  }

  @SuppressWarnings("unchecked")
  private IDocumentFactory<WritableComparable, Writable> getDocumentFactory(final JobConf configuration) {
    IDocumentFactory<WritableComparable, Writable> factory;
    final String converterClass = configuration.get(IndexJobConf.DOCUMENT_FACTORY_CLASS);
    try {
      final Class<IDocumentFactory> clazz = (Class<IDocumentFactory>) Class.forName(converterClass);
      factory = clazz.newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("can not create document factory '" + converterClass
          + "', because it is not available", e);
    }
    try {
      factory.configure(configuration);
    } catch (final IOException e) {
      throw new RuntimeException(
          "exception while configure the document factory'" + factory.getClass().getName() + "'", e);
    }
    return factory;
  }

}
