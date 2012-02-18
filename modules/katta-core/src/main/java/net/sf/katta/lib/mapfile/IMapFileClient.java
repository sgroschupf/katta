/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.lib.mapfile;

import java.util.List;

import net.sf.katta.util.KattaException;

/**
 * The public interface to the front end of the MapFile server.
 */
public interface IMapFileClient {

  /**
   * Get all entries with the given key.
   * 
   * @param key The entry(s) to look up.
   * @param indexNames The MapFiles to search.
   * @return All the entries with the given key.
   * @throws KattaException 
   */
  public List<String> get(String key, final String[] indexNames) throws KattaException;

  /**
   * Closes down the client.
   */
  public void close();

}