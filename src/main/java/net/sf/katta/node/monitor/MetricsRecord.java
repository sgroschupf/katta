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
package net.sf.katta.node.monitor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MetricsRecord implements Serializable {
  private static final long serialVersionUID = 1832158671826263268L;

  private List<Record> _records = new ArrayList<Record>();

  private final String _serverId;

  public MetricsRecord(String serverId) {
    _serverId = serverId;
  }

  public String getServerId() {
    return _serverId;
  }

  public void addValue(String key, long value, long timeStamp) {
    _records.add(new Record(key, value, timeStamp));
  }

  public List<Record> getRecords() {
    return _records;
  }

  public class Record implements Serializable {

    private static final long serialVersionUID = 8134090886018804896L;

    private final String _key;
    private final long _value;
    private final long _timeStamp;

    public Record(String key, long value, long timeStamp) {
      _key = key;
      _value = value;
      _timeStamp = timeStamp;
    }

    public String getKey() {
      return _key;
    }

    public long getValue() {
      return _value;
    }

    public long getTimeStamp() {
      return _timeStamp;
    }

    @Override
    public String toString() {
      return _key + " " + _value + " " + _timeStamp;
    }
  }

  @Override
  public String toString() {
    return _serverId + ": " + _records;
  }
}
