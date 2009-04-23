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


public class LoadTestNodeConfiguration extends KattaConfiguration {

  private final static String LOADTESTNODE_TEST_DELAY_MS = "loadtestnode.testdelay";
  private static final String LOADTESTNODE_STATISTICS_OUTPUT_FILE = "loadtestnode.statistics.output";
  private static final String LOADTESTNODE_START_PORT = "loadtestnode.server.port.start";

  public LoadTestNodeConfiguration() {
    super("/katta.loadtestnode.properties");
  }

  public int getTestDelay() {
    return getInt(LOADTESTNODE_TEST_DELAY_MS);
  }

  public void setTestDelay(int delayMs) {
    setProperty(LOADTESTNODE_TEST_DELAY_MS, delayMs + "");
  }

  public String getStatisticsFile() {
    return getProperty(LOADTESTNODE_STATISTICS_OUTPUT_FILE);
  }
  
  public void setStatisticsFile(String fileName) {
    setProperty(LOADTESTNODE_STATISTICS_OUTPUT_FILE, fileName);
  }

  public int getStartPort() {
    return getInt(LOADTESTNODE_START_PORT);
  }

  public void setStartPort(int startPort) {
    setProperty(LOADTESTNODE_START_PORT, startPort + "");
  }
}
