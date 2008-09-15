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

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.log4j.Logger;

public class VersionInfo {

  private static final Logger LOG = Logger.getLogger(VersionInfo.class);

  /**
   * The version of Katta.
   */
  public static final String VERSION;

  public static final String SVN_URL;
  public static final String SVN_REVISION;

  public static final String COMPILED_BY;
  public static final String COMPILE_TIME;

  static {
    String jar = findContainingJar(VersionInfo.class);
    if (jar != null) {
      LOG.debug("load version info from '" + jar + "'");
      final Manifest manifest = getManifest(jar);

      final Attributes mainAttributes = manifest.getMainAttributes();
      VERSION = mainAttributes.getValue("Implementation-Version");
      SVN_URL = mainAttributes.getValue("SVN-URL");
      SVN_REVISION = mainAttributes.getValue("SVN-Revision");
      COMPILED_BY = mainAttributes.getValue("Compiled-By");
      COMPILE_TIME = mainAttributes.getValue("Compile-Time");
    } else {
      LOG.debug("could not find katta jar - setting version infos to unknown");
      VERSION = "Unknown";
      SVN_URL = "Unknown";
      SVN_REVISION = "Unknown";
      COMPILED_BY = "Unknown";
      COMPILE_TIME = "Unknown";
    }
  }

  private static Manifest getManifest(String jar) {
    try {
      final JarFile jarFile = new JarFile(jar);
      final Manifest manifest = jarFile.getManifest();
      return manifest;
    } catch (Exception e) {
      throw new RuntimeException("could not load manifest from jar " + jar);
    }
  }

  private static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

}
