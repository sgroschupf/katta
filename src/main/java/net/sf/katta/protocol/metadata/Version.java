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
package net.sf.katta.protocol.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.log4j.Logger;

/**
 * Version of a cluster or distribution (depending if loaded from jar or from
 * the cluster itself).
 */
public class Version implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = Logger.getLogger(Version.class);

  /**
   * The version of Katta.
   */
  private final String _number;
  private final String _revision;
  private final String _compiledBy;
  private final String _compileTime;

  public Version(String number, String revision, String compiledBy, String compileTime) {
    _number = number;
    _revision = revision;
    _compiledBy = compiledBy;
    _compileTime = compileTime;
  }

  public String getNumber() {
    return _number;
  }

  public String getRevision() {
    return _revision;
  }

  public String getCompiledBy() {
    return _compiledBy;
  }

  public String getCompileTime() {
    return _compileTime;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_number == null) ? 0 : _number.hashCode());
    result = prime * result + ((_revision == null) ? 0 : _revision.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Version other = (Version) obj;
    if (_number == null) {
      if (other._number != null)
        return false;
    } else if (!_number.equals(other._number))
      return false;
    if (_revision == null) {
      if (other._revision != null)
        return false;
    } else if (!_revision.equals(other._revision))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return getNumber() + " | " + getRevision() + " | " + getCompileTime() + " | by " + getCompiledBy();
  }

  public static Version readFromJar() {
    String jar = findContainingJar(Version.class);
    String number;
    String revision;
    String compiledBy;
    String compileTime;
    if (jar != null) {
      LOG.debug("load version info from '" + jar + "'");
      final Manifest manifest = getManifest(jar);

      final Map<String, Attributes> attrs = manifest.getEntries();
      Attributes attr = attrs.get("net/sf/katta");
      number = attr.getValue("Implementation-Version");
      revision = attr.getValue("Git-Revision");
      compiledBy = attr.getValue("Compiled-By");
      compileTime = attr.getValue("Compile-Time");
    } else {
      LOG.warn("could not find katta jar - setting version infos to unknown");
      number = "Unknown";
      revision = "Unknown";
      compiledBy = "Unknown";
      compileTime = "Unknown";
    }
    return new Version(number, revision, compiledBy, compileTime);
  }

  private static String findContainingJar(Class<?> clazz) {
    ClassLoader loader = clazz.getClassLoader();
    String className = clazz.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> enumeration = loader.getResources(className); enumeration.hasMoreElements();) {
        URL url = enumeration.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          return path.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
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
}
