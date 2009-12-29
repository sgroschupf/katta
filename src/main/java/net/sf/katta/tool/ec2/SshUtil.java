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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class SshUtil {

  public static void scp(String keyPath, String sourcePath, String targetHostName, String targetFileName)
          throws IOException {
    try {
      File keyFile = new File(keyPath);
      JSch jsch = new JSch();
      jsch.addIdentity(keyFile.getAbsolutePath());
      Session session = jsch.getSession("root", targetHostName, 22);
      UserInfo ui = new SshUser();
      session.setUserInfo(ui);
      session.connect();

      String command = "scp -p -t  " + targetFileName;
      Channel channel = session.openChannel("exec");
      ((ChannelExec) channel).setCommand(command);

      // get I/O streams for remote scp
      OutputStream out = channel.getOutputStream();
      InputStream in = channel.getInputStream();

      channel.connect();

      if (checkAck(in) != 0) {
        throw new RuntimeException("Failed to scp key file");
      }

      // send "C0644 filesize filename", where filename should not include
      // '/'
      long filesize = (new File(sourcePath)).length();
      command = "C0644 " + filesize + " ";
      if (sourcePath.lastIndexOf('/') > 0) {
        command += sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
      } else {
        command += sourcePath;
      }
      command += "\n";
      out.write(command.getBytes());
      out.flush();
      if (checkAck(in) != 0) {
        // this actually will never happend since checkAck already
        // throws an exception now.
        throw new RuntimeException("Fatal Error scp key file");
      }
      // send a content of lfile
      FileInputStream fis = new FileInputStream(sourcePath);
      byte[] buf = new byte[1024];
      while (true) {
        int len = fis.read(buf, 0, buf.length);
        if (len <= 0)
          break;
        out.write(buf, 0, len); // out.flush();
      }
      fis.close();
      fis = null;
      // send '\0'
      buf[0] = 0;
      out.write(buf, 0, 1);
      out.flush();
      if (checkAck(in) != 0) {
        throw new RuntimeException("Failed to scp  file");
      }
      out.close();
      channel.disconnect();
      session.disconnect();
    } catch (JSchException e) {
      throw new IOException("Unable to copy key file to host: ", e);
    }
  }

  public static boolean sshRemoteCommand(String dnsName, String command, String keyPath) throws IOException {
    try {
      File file = new File(keyPath);
      JSch jsch = new JSch();
      jsch.addIdentity(file.getAbsolutePath());
      Session session = jsch.getSession("root", dnsName, 22);
      UserInfo ui = new SshUser();
      session.setUserInfo(ui);
      session.connect();

      Channel channel = session.openChannel("exec");

      ((ChannelExec) channel).setCommand(command);
      ((ChannelExec) channel).setErrStream(System.out);
      InputStream in = channel.getInputStream();
      channel.connect();
      byte[] tmp = new byte[1024];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, 1024);
          if (i < 0)
            break;
          // we acully dont want to show the output
          // System.out.print(new String(tmp, 0, i));
        }
        if (channel.isClosed()) {
          // System.out.println("exit-status: " +
          // channel.getExitStatus());
          if (channel.getExitStatus() != 0) {
            return false;
          }
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ee) {
          Thread.currentThread().interrupt();
        }
      }
      channel.disconnect();
      session.disconnect();
    } catch (JSchException e) {
      return false;
      // throw new IOException("Unable to ssh into master", e);
    }
    return true;
  }

  static class SshUser implements UserInfo {

    @Override
    public String getPassphrase() {
      return "";
    }

    @Override
    public String getPassword() {
      return "";
    }

    @Override
    public boolean promptPassphrase(String arg0) {
      return true;
    }

    @Override
    public boolean promptPassword(String arg0) {
      return true;
    }

    @Override
    public boolean promptYesNo(String arg0) {
      return true;
    }

    @Override
    public void showMessage(String arg0) {
      System.out.println(arg0);
    }

  }

  /**
   * from the jsch examples
   */
  private static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    // 1 for error,
    // 2 for fatal error,
    // -1
    if (b == 0)
      return b;
    if (b == -1)
      return b;

    if (b == 1 || b == 2) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      } while (c != '\n');
      if (b == 1) { // error
        System.err.println("Ec2Hadoop.checkAck(): " + sb.toString());
        throw new IOException("Error scp file: " + sb.toString());
        // System.out.print(sb.toString());
      }
      if (b == 2) { // fatal error
        throw new IOException("Fatal error scp key file: " + sb.toString());
        // System.out.print(sb.toString());
      }
    }
    return b;
  }

}
