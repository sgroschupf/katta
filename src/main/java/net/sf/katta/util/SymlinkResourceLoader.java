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
import java.net.MalformedURLException;
import java.net.URL;

/**
 * With help of this class a file outside or inside the classpath can be made
 * available to the classpath under a given name.
 * 
 */
public class SymlinkResourceLoader extends ClassLoader {

  private final String _symlink;

  private final URL _target;

  public SymlinkResourceLoader(ClassLoader parentLoader, String symlink, File target) {
    super(parentLoader);
    _symlink = symlink;
    try {
      _target = target.toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException("could not create url out of '" + target + "'", e);
    }
  }

  @Override
  public URL getResource(String name) {
    if (_symlink.equals(name)) {
      return _target;
    }
    return super.getResource(name);
  }

  @Override
  protected URL findResource(String name) {
    if (_symlink.equals(name)) {
      return _target;
    }
    return super.findResource(name);
  }

}
