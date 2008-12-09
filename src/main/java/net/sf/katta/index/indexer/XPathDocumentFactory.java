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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Set;

import net.sf.katta.util.NumberPaddingUtil;
import net.sf.katta.util.PropertyUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

public class XPathDocumentFactory implements IDocumentFactory<Text, Text> {

  private final static Logger LOG = Logger.getLogger(XPathDocumentFactory.class);

  public static final String XPATH_INPUT_FILE = "xpath.input.file";
  private IXPathService _xPathService = new DefaultXPathService();
  private Properties _properties = new Properties();
  private static final SimpleDateFormat _dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");

  public Document convert(final Text key, final Text value) {
    final Document document = new Document();
    // TODO jz: we should re-use the Field instances (see
    // http://wiki.apache.org/lucene-java/ImproveIndexingSpeed)

    // TODO jz: use value.getBytes() instead ??
    final Field content = new Field("content", value.toString(), Store.YES, Index.NO);
    document.add(content);
    try {
      final Set<Object> keySet = _properties.keySet();
      for (final Object xpath : keySet) {
        final Object parsedValueType = _properties.get(xpath);
        String parsedValue = _xPathService.parse(xpath.toString(), value.toString());
        if ("NUMBER".equals(parsedValueType)) {
          parsedValue = NumberPaddingUtil.padding(Double.parseDouble(parsedValue));
        } else if ("DATE".equals(parsedValueType)) {
          final Date date = _dateFormat.parse(parsedValue);
          final GregorianCalendar calendar = new GregorianCalendar();
          calendar.setTime(date);
          parsedValue = "" + calendar.get(Calendar.YEAR) + calendar.get(Calendar.MONTH)
              + calendar.get(Calendar.DAY_OF_MONTH);
        }
        final Field field = new Field("" + xpath.hashCode(), parsedValue, Store.YES, Index.UN_TOKENIZED);
        document.add(field);
      }
    } catch (final Exception e) {
      LOG.warn("can not create document", e);
    }
    return document;
  }

  public Analyzer getIndexAnalyzer() {
    // TODO correct analyzer
    return new StandardAnalyzer();
  }

  public void setXPathService(final IXPathService pathService) {
    _xPathService = pathService;
  }

  public void configure(final JobConf jobConf) throws IOException {
    final String fileName = jobConf.get(XPATH_INPUT_FILE);
    if (fileName != null) {
      final Path xpathInputFile = new Path(fileName);
      final FileSystem fileSystem = FileSystem.get(jobConf);
      final String tmp = System.getProperty("java.io.tmpdir");
      final Path tmpFile = new Path(tmp, XPathDocumentFactory.class.getName());
      fileSystem.copyToLocalFile(xpathInputFile, tmpFile);
      _properties = PropertyUtil.loadProperties(new File(tmpFile.toString()));
    } else {
      _properties = PropertyUtil.loadProperties("/xpath.properties");
    }
  }

}
