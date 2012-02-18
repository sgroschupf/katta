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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class PropertyUtil {

  public static Properties loadProperties(String path) {
    InputStream inputStream = PropertyUtil.class.getResourceAsStream(path);
    if (inputStream == null) {
      throw new RuntimeException(path + " not in classpath");
    }
    final Properties properties = new Properties();
    try {
      properties.load(inputStream);
      return properties;
    } catch (final IOException e) {
      throw new RuntimeException("unable to load kata.properties", e);
    }
  }

  public static String getPropertiesFilePath(final String path) {
    URL resource = PropertyUtil.class.getResource(path);
    if (resource == null) {
      throw new RuntimeException(path + " not in classpath");
    }
    return resource.toString();
  }

  public static Properties loadProperties(final File file) {
    final Properties properties = new Properties();
    try {
      FileInputStream inStream = new FileInputStream(file);
      properties.load(inStream);
      return properties;
    } catch (final IOException e) {
      throw new RuntimeException("unable to load kata.properties", e);
    }
  }

}
