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
 

import java.io.File;
import java.io.FileInputStream;
import java.security.MessageDigest;


class Md5 {
  
  public static void md5(File inFile, File outFile) {
    MessageDigest algorithm = MessageDigest.getInstance("MD5");
    algorithm.reset();
    
    FileInputStream fileInputStream = new FileInputStream(inFile);
    int read = -1;
    byte[] buffer = new byte[1024];
    while ((read = fileInputStream.read(buffer, 0, 1024)) > -1) {
      algorithm.update(buffer, 0, read);
    }
    byte[] digest = algorithm.digest();
    String hexString = new BigInteger(1, digest).toString(16);
    fileInputStream.close();
    FileOutputStream fileOutputStream = new FileOutputStream(outFile);
    fileOutputStream.write(hexString.toString().getBytes());
    fileOutputStream.close();
  }
}