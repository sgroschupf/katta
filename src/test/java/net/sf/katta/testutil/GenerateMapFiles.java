/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.testutil;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class GenerateMapFiles {

  /**
   * This generates the very simple MapFiles in katta/src/test/testMapFile[AB]/.
   * These files are supposed to simulate taking 2 large MapFiles and splitting the first one
   * into 4 shards, the second into 2 shards. We do not provide such a tool yet.
   * The results are checked in, so you should not need to run this. Is is provided
   * as a reference.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("io.file.buffer.size", "4096");
    FileSystem fs = new RawLocalFileSystem();
    fs.setConf(conf);
    //
    File f = new File("src/test/testMapFileA/a1");
    MapFile.Writer w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "a.txt", "This is a test");
    write(w, "b.xml", "<name>test</name>");
    write(w, "c.log", "1/1/2009: test");
    w.close();
    //
    f = new File("src/test/testMapFileA/a2");
    w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "d.html", "<b>test</b>");
    write(w, "e.txt", "An e test");
    write(w, "f.log", "1/2/2009: test2");
    w.close();
    //
    f = new File("src/test/testMapFileA/a3");
    w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "g.log", "1/3/2009: more test");
    write(w, "h.txt", "Test in part 3");
    w.close();
    //
    f = new File("src/test/testMapFileA/a4");
    w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "i.xml", "<i>test</i>");
    write(w, "j.log", "1/4/2009: 4 test");
    write(w, "k.out", "test data");
    write(w, "l.txt", "line 4");
    w.close();
    //
    //
    f = new File("src/test/testMapFileB/b1");
    w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "u.txt", "Test U text");
    write(w, "v.xml", "<victor>foo</victor>");
    write(w, "w.txt", "where is test");
    w.close();
    //
    f = new File("src/test/testMapFileB/b2");
    w = new MapFile.Writer(conf, fs, f.getAbsolutePath(), Text.class, Text.class);
    write(w, "x.txt", "xrays ionize");
    write(w, "y.xml", "<yankee>foo</yankee>");
    write(w, "z.xml", "<zed>foo</zed>");
    w.close();
  }
  
  private static void write(MapFile.Writer w, String fn, String data) throws Exception {
    w.append(new Text(fn), new Text(data));
  }
  
}
