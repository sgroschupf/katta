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
package net.sf.katta.loadtest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TestSearcherMetaData implements Writable {

  private Text _host = new Text();
  private int _port = 17676;
  
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    _host.readFields(dataInput);
    _port = dataInput.readInt();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    _host.write(dataOutput);
    dataOutput.writeInt(_port);
  }

  public String getHost() {
    return _host.toString();
  }

  public void setHost(String host) {
    _host.set(host);
  }

  public int getPort() {
    return _port;
  }

  public void setPort(int port) {
    _port = port;
  }
}
