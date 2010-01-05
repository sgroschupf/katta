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
package net.sf.katta.util;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

public class KattaConfiguration implements Serializable {

  private static final long serialVersionUID = 1L;
  protected Properties _properties;
  private final String _resourcePath;

  public KattaConfiguration(final String path) {
    _properties = PropertyUtil.loadProperties(path);
    _resourcePath = PropertyUtil.getPropertiesFilePath(path);
  }

  public KattaConfiguration(File file) {
    _properties = PropertyUtil.loadProperties(file);
    _resourcePath = file.getAbsolutePath();
  }

  public KattaConfiguration(Properties properties, String filePath) {
    _properties = properties;
    _resourcePath = filePath;
  }

  public KattaConfiguration() {
    _properties = new Properties();
    _resourcePath = null;
  }

  public String getResourcePath() {
    return _resourcePath;
  }

  public boolean containsProperty(final String key) {
    return _properties.containsKey(key);
  }

  public String getProperty(final String key) {
    final String value = _properties.getProperty(key);
    if (value == null) {
      throw new IllegalStateException("no property with key '" + key + "' found");
    }
    return value;
  }

  public String getProperty(final String key, final String defaultValue) {
    String value = _properties.getProperty(key);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  protected void setProperty(String key, String value) {
    _properties.setProperty(key, value);
  }

  protected void setProperty(String key, long value) {
    _properties.setProperty(key, Long.toString(value));
  }

  public int getInt(final String key) {
    return Integer.parseInt(getProperty(key));
  }

  public int getInt(final String key, final int defaultValue) {
    try {
      return Integer.parseInt(getProperty(key));
    } catch (NumberFormatException e) {
      return defaultValue;
    } catch (IllegalStateException e) {
      return defaultValue;
    }
  }

  public File getFile(final String key) {
    return new File(getProperty(key));
  }

  public Class<?> getClass(final String key) {
    final String className = getProperty(key);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("can not create class " + className, e);
    }
  }

  public Set<String> getKeys() {
    return _properties.stringPropertyNames();
  }
}
