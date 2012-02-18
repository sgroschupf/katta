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
package net.sf.katta.tool.ec2;

public class Ec2Instance {
  private final String _privateDnsName;
  private final String _publicDnsName;

  public Ec2Instance(String privateDnsName, String publicdnsName) {
    _privateDnsName = privateDnsName;
    _publicDnsName = publicdnsName;
  }

  public String getInternalHost() {
    return _privateDnsName;
  }

  public String getExternalHost() {
    return _publicDnsName;
  }

}
