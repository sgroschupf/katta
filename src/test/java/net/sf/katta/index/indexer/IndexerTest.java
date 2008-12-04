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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;
import net.sf.katta.index.indexer.Indexer.DocumentCounter;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class IndexerTest extends TestCase {

  protected static File _folder = new File(System.getProperty("java.io.tmpdir") + File.separator
      + IndexerTest.class.getName());
  private final Mockery _mockery = new Mockery();

  public static class DummyFactory implements IDocumentFactory<WritableComparable, Writable> {
    public Document convert(final WritableComparable key, final Writable value) {
      final Field field = new Field("foo", "bar", Store.YES, Index.TOKENIZED);
      final Document document = new Document();
      document.add(field);
      return document;
    }

    public Analyzer getIndexAnalyzer() {
      return new StandardAnalyzer();
    }

    public void configure(final JobConf jobConf) throws IOException {
      // nothing todo
    }
  }

  public static class DummyDistributer implements IIndexPublisher {

    public void configure(final JobConf jobConf) {
      // nothing todo
    }

    public void publish(final String pathToIndex) {
      try {
        final File file = new File(_folder, "copy.done");
        file.createNewFile();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class DummyDistributer2 implements IIndexPublisher {

    private JobConf _jobConf;

    public void configure(final JobConf jobConf) {
      _jobConf = jobConf;
    }

    public void publish(final String pathToIndex) {
      try {
        assertTrue(new File(_jobConf.get(IndexConfiguration.INDEX_TMP_DIRECTORY)).exists());
        final File file = new File(_jobConf.get(IndexConfiguration.INDEX_TMP_DIRECTORY));
        final IndexReader indexReader = IndexReader.open(file.listFiles()[0].listFiles()[0]);
        assertEquals(1, indexReader.maxDoc());
        final Document document = indexReader.document(0);
        assertEquals("bar", document.get("foo"));
        indexReader.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class DummyZipper implements IZipService {

    public boolean zipFolder(final File inputFolder, final File outputFile) {
      try {
        final File file = new File(_folder, "zip.done");
        file.createNewFile();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      return true;
    }

  }

  public static class DummyWritable implements Writable {

    public void write(final DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(Integer.MAX_VALUE);
    }

    public void readFields(final DataInput dataInput) throws IOException {
      dataInput.readInt();
    }
  }

  public static class DummyWritableComparable implements WritableComparable {

    public void write(final DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(Integer.MIN_VALUE);
    }

    public void readFields(final DataInput dataInput) throws IOException {
      dataInput.readInt();
    }

    public int compareTo(final Object o) {

      return 0;
    }
  }

  public void testIndexerComplete() throws Exception {

    final Indexer mapRunnable = new Indexer();
    final JobConf jobConf = new JobConf();
    jobConf.set(IndexConfiguration.DOCUMENT_FACTORY_CLASS, DummyFactory.class.getName());
    jobConf.set(IndexConfiguration.INDEX_TMP_DIRECTORY, _folder.getAbsolutePath() + File.separator + "index");
    jobConf.set(IndexConfiguration.INDEX_PUBLISHER_CLASS, DummyDistributer.class.getName());
    jobConf.set(IndexConfiguration.INDEX_ZIP_CLASS, DummyZipper.class.getName());
    jobConf.set(IndexConfiguration.INPUT_KEY_CLASS, DummyWritableComparable.class.getName());
    jobConf.set(IndexConfiguration.INPUT_VALUE_CLASS, DummyWritable.class.getName());

    mapRunnable.configure(jobConf);

    final OutputCollector<WritableComparable, Writable> outputCollector = _mockery.mock(OutputCollector.class);
    final Reporter reporter = _mockery.mock(Reporter.class);
    final WritableComparable writableComparable = _mockery.mock(WritableComparable.class);
    final Iterator iterator = _mockery.mock(Iterator.class);
    final DataOutputBuffer buffer = new DataOutputBuffer();
    final DummyWritableComparable keyValue = new DummyWritableComparable();
    final DummyWritable valueValue = new DummyWritable();
    keyValue.write(buffer);
    valueValue.write(buffer);
    final BytesWritable bytesWritable = new BytesWritable(buffer.getData());

    _mockery.checking(new Expectations() {
      {
        one(iterator).hasNext();
        will(returnValue(true));
        one(iterator).next();
        will(returnValue(bytesWritable));
        one(iterator).hasNext();
        will(returnValue(false));

        atLeast(1).of(reporter).setStatus(with(any(String.class)));
        one(reporter).incrCounter(DocumentCounter.INDEXED_DOCUMENT_COUNT, 1);
        one(reporter).incrCounter(DocumentCounter.DOCUMENT_COUNT, 1);

      }
    });

    mapRunnable.reduce(writableComparable, iterator, outputCollector, reporter);

    assertTrue(new File(jobConf.get(IndexConfiguration.INDEX_TMP_DIRECTORY)).exists());
    final File file = new File(jobConf.get(IndexConfiguration.INDEX_TMP_DIRECTORY));
    assertEquals(0, file.listFiles().length);
    assertTrue(new File(_folder, "zip.done").exists());
    assertTrue(new File(_folder, "copy.done").exists());

  }

  public void testIndexer() throws Exception {

    final Indexer mapRunnable = new Indexer();
    final JobConf jobConf = new JobConf();
    jobConf.set(IndexConfiguration.DOCUMENT_FACTORY_CLASS, DummyFactory.class.getName());
    jobConf.set(IndexConfiguration.INDEX_TMP_DIRECTORY, _folder.getAbsolutePath() + File.separator + "index");
    jobConf.set(IndexConfiguration.INDEX_PUBLISHER_CLASS, DummyDistributer2.class.getName());
    jobConf.set(IndexConfiguration.INDEX_ZIP_CLASS, DummyZipper.class.getName());
    jobConf.set(IndexConfiguration.INPUT_KEY_CLASS, DummyWritableComparable.class.getName());
    jobConf.set(IndexConfiguration.INPUT_VALUE_CLASS, DummyWritable.class.getName());

    mapRunnable.configure(jobConf);

    final OutputCollector<WritableComparable, Writable> outputCollector = _mockery.mock(OutputCollector.class);
    final Reporter reporter = _mockery.mock(Reporter.class);
    final WritableComparable writableComparable = _mockery.mock(WritableComparable.class);
    final Iterator iterator = _mockery.mock(Iterator.class);
    final DataOutputBuffer buffer = new DataOutputBuffer();
    final DummyWritableComparable keyValue = new DummyWritableComparable();
    final DummyWritable valueValue = new DummyWritable();
    keyValue.write(buffer);
    valueValue.write(buffer);
    final BytesWritable bytesWritable = new BytesWritable(buffer.getData());

    _mockery.checking(new Expectations() {
      {
        one(iterator).hasNext();
        will(returnValue(true));
        one(iterator).next();
        will(returnValue(bytesWritable));
        one(iterator).hasNext();
        will(returnValue(false));

        atLeast(1).of(reporter).setStatus(with(any(String.class)));
        one(reporter).incrCounter(DocumentCounter.INDEXED_DOCUMENT_COUNT, 1);
        one(reporter).incrCounter(DocumentCounter.DOCUMENT_COUNT, 1);
      }
    });

    mapRunnable.reduce(writableComparable, iterator, outputCollector, reporter);
  }

  @Override
  protected void tearDown() throws Exception {
    assertTrue(FileUtil.fullyDelete(_folder));

  }
}
