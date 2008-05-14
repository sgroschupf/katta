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
package net.sf.katta.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IndexMetaData implements Writable {

  private Text _path;

  private Text _analyzerClassName;

  private BooleanWritable _isDeployed;

  public IndexMetaData(final String path, final String analyzerName, final boolean isDeployed) {
    _path = new Text(path);
    _analyzerClassName = new Text(analyzerName);
    _isDeployed = new BooleanWritable(isDeployed);
  }

  public IndexMetaData() {
  }

  public void readFields(final DataInput in) throws IOException {
    _path = new Text();
    _path.readFields(in);
    _analyzerClassName = new Text();
    _analyzerClassName.readFields(in);
    _isDeployed = new BooleanWritable();
    _isDeployed.readFields(in);

  }

  public void write(final DataOutput out) throws IOException {
    _path.write(out);
    _analyzerClassName.write(out);
    _isDeployed.write(out);
  }

  public String getPath() {
    return _path.toString();
  }

  public String getAnalyzerClassName() {
    return _analyzerClassName.toString();
  }

  public boolean isDeployed() {
    return _isDeployed.get();
  }

  public void setIsDeployed(final boolean b) {
    _isDeployed.set(b);
  }

}
