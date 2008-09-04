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
package net.sf.katta.util;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

public class KattaConfiguration {

  protected Properties _properties;

  public KattaConfiguration(final String path) {
    _properties = PropertyUtil.loadProperties(path);
  }

  public KattaConfiguration(File file) {
    _properties = PropertyUtil.loadProperties(file);
  }

  public boolean containsProperty(final String key) {
    return _properties.contains(key);
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

  public int getInt(final String key) {
    return Integer.parseInt(getProperty(key));
  }

  public File getFile(final String key) {
    return new File(getProperty(key));
  }

  public Path getPath(final String key) {
    return new Path(getProperty(key));
  }

  public Class<?> getClass(final String key) {
    final String className = getProperty(key);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("can not create class " + className, e);
    }
  }

}
