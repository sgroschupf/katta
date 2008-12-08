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
package net.sf.katta.tool;

import java.util.List;

import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ZkTool {

  private ZKClient _zkClient;

  public ZkTool() throws KattaException {
    _zkClient = new ZKClient(new ZkConfiguration());
    _zkClient.start(5000);
  }

  public void ls(String path) throws KattaException {
    List<String> children = _zkClient.getChildren(path);
    System.out.println(String.format("Found %s items", children.size()));
    if (path.charAt(path.length() - 1) != ZkPathes.SEPERATOR) {
      path += ZkPathes.SEPERATOR;
    }
    for (String child : children) {
      System.out.println(path + child);
    }
  }

  public void rm(String path, boolean recursiv) throws KattaException {
    if (recursiv) {
      _zkClient.deleteRecursive(path);
    } else {
      _zkClient.delete(path);
    }
  }

  private void close() {
    _zkClient.close();
  }

  public static void main(String[] args) throws KattaException {
    final Options options = new Options();

    Option lsOption = new Option("ls", true, "list zp path contents");
    lsOption.setArgName("path");
    Option rmOption = new Option("rm", true, "remove zk files");
    rmOption.setArgName("path");
    Option rmrOption = new Option("rmr", true, "remove zk directories");
    rmrOption.setArgName("path");

    OptionGroup actionGroup = new OptionGroup();
    actionGroup.setRequired(true);
    actionGroup.addOption(lsOption);
    actionGroup.addOption(rmOption);
    actionGroup.addOption(rmrOption);
    options.addOptionGroup(actionGroup);

    final CommandLineParser parser = new GnuParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      final CommandLine line = parser.parse(options, args);
      ZkTool zkTool = new ZkTool();
      if (line.hasOption(lsOption.getOpt())) {
        String path = lsOption.getValue(0);
        zkTool.ls(path);
      } else if (line.hasOption(rmOption.getOpt())) {
        String path = rmOption.getValue(0);
        zkTool.rm(path, false);
      } else if (line.hasOption(rmrOption.getOpt())) {
        String path = rmrOption.getValue(0);
        zkTool.rm(path, true);
      }
      zkTool.close();
    } catch (ParseException e) {
      System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
      formatter.printHelp(ZkTool.class.getSimpleName(), options);
    }

  }

}
