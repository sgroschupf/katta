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

/**
 * Contains hadoop configuration keys used in
 * hadoop-default.xml/hadoop-site.xml.
 */
public interface IHadoopConstants {

  static final String NAMENODE = "fs.default.name";
  static final String JOBTRACKER = "mapred.job.tracker";

  public static final String TMP_DIR = "hadoop.tmp.dir";

  public static final String NATIVE_LIB = "hadoop.native.lib";

  public static final String NUMBER_OF_MAP_TASKS_PER_JOB = "mapred.map.tasks";
  public static final String NUMBER_OF_REDUCE_TASKS_PER_JOB = "mapred.reduce.tasks";
  public static final String NUMBER_OF_MAP_TASKS_PER_TASKTRACKER = "mapred.tasktracker.map.tasks.maximum";
  public static final String NUMBER_OF_REDUCE_TASKS_PER_TASKTRACKER = "mapred.tasktracker.reduce.tasks.maximum";
  public static final String CHILD_JVM_OPTS = "mapred.child.java.opts";

  public static final String MAPRED_LOCAL_DIR = "mapred.local.dir";
  public static final String MAPRED_SYSTEM_DIR = "mapred.system.dir";
  public static final String MAPRED_TEMP_DIR = "mapred.temp.dir";
}
