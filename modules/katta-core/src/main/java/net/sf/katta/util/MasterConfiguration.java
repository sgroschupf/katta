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

@SuppressWarnings("serial")
public class MasterConfiguration extends KattaConfiguration {

  public final static String DEPLOY_POLICY = "master.deploy.policy";
  public final static String SAFE_MODE_MAX_TIME = "safemode.maxTime";

  public MasterConfiguration() {
    super("/katta.master.properties");
  }

  public MasterConfiguration(File file) {
    super(file);
  }

  public String getDeployPolicy() {
    return getProperty(DEPLOY_POLICY, "net.sf.katta.master.DefaultDistributionPolicy");
  }

}
