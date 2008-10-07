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

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;

public class ConfigurableDocumentDuplicateInformation implements IDocumentDuplicateInformation {

  public static final String CONF_KEY_DOCUMENT_FIELDS = ConfigurableDocumentDuplicateInformation.class.getName()
      + ".documentFields";
  public static final String CONF_KEY_KEY_FIELD = ConfigurableDocumentDuplicateInformation.class.getName()
      + ".keyField";
  public static final String CONF_KEY_SORT_FIELD = ConfigurableDocumentDuplicateInformation.class.getName()
      + ".sortField";

  private Configuration _conf;

  private String[] _documentFields;
  private String _sortField;
  private String _keyField;

  public String getKey(Document document) {
    return document.get(_keyField);
  }

  public String getSortValue(Document document) {
    return document.get(_sortField);
  }

  public String[] getSupportedFieldNames() {
    return _documentFields;
  }

  public Configuration getConf() {
    return _conf;
  }

  public void setConf(Configuration conf) {
    _conf = conf;
    _documentFields = get(conf, CONF_KEY_DOCUMENT_FIELDS).split(",");
    _keyField = get(conf, CONF_KEY_KEY_FIELD);
    _sortField = get(conf, CONF_KEY_SORT_FIELD);
  }

  private String get(Configuration conf, String key) {
    String confValue = conf.get(key);
    if (confValue == null) {
      throw new IllegalStateException("configuration key '" + key + "' not found");
    }
    return confValue;
  }

}
