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

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;

public class WebApp {

  private final String _warPath;
  private int _port;

  public WebApp(String[] paths, int port) {
    _warPath = findWarInPathOrChilds(paths);
    _port = port;
  }

  private String findWarInPathOrChilds(String[] warPaths) {
    for (String path : warPaths) {
      File file = new File(path);
      if (isWarFile(file)) {
        return file.getAbsolutePath();
      }
      File[] listFiles = file.listFiles();
      if (listFiles != null) {
        for (File subFiles : listFiles) {
          if (isWarFile(subFiles)) {
            return subFiles.getAbsolutePath();
          }
        }
      }
    }
    throw new IllegalArgumentException("Unable to find war");
  }

  private boolean isWarFile(File file) {
    return file.exists() && file.getName().endsWith(".war");
  }

  public void startWebServer() throws Exception {

    Server server = new Server();
    Connector connector = new SelectChannelConnector();
    connector.setPort(_port);
    server.setConnectors(new Connector[] { connector });

    WebAppContext webapp = new WebAppContext();
    webapp.setContextPath("/");
    webapp.setWar(_warPath);
    server.setHandler(webapp);

    server.start();
    server.join();
  }

  public static void main(String[] args) throws Exception {
    WebApp webApp = new WebApp(new String[] { args[0] }, Integer.parseInt(args[1]));
    webApp.startWebServer();
  }

}
