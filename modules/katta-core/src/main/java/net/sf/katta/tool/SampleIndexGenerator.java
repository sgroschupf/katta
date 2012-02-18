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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Generates a test index, for example used for benchmarking
 */
public class SampleIndexGenerator {

  public void createIndex(String input, String output, int wordsPerDoc, int indexSize) {
    createIndex(getWordList(input), output, wordsPerDoc, indexSize);
  }

  public void createIndex(String[] wordList, String output, int wordsPerDoc, int indexSize) {
    long startTime = System.currentTimeMillis();
    String hostname = "unknown";
    InetAddress addr;
    try {
      addr = InetAddress.getLocalHost();
      hostname = addr.getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to get localhostname", e);
    }

    File index = new File(output, hostname + "-" + UUID.randomUUID().toString());
    int count = wordList.length;
    Random random = new Random(System.currentTimeMillis());
    try {
      IndexWriter indexWriter = new IndexWriter(FSDirectory.open(index), new StandardAnalyzer(Version.LUCENE_30), true,
              MaxFieldLength.UNLIMITED);
      for (int i = 0; i < indexSize; i++) {
        // generate text first
        StringBuffer text = new StringBuffer();
        for (int j = 0; j < wordsPerDoc; j++) {
          text.append(wordList[random.nextInt(count)]);
          text.append(" ");
        }

        Document document = new Document();
        document.add(new Field("key", hostname + "_" + i, Store.NO, Index.NOT_ANALYZED));
        document.add(new Field("text", text.toString(), Store.NO, Index.ANALYZED));
        indexWriter.addDocument(document);

      }
      indexWriter.optimize();
      indexWriter.close();
      System.out.println("Index created with : " + indexSize + " documents in "
              + (System.currentTimeMillis() - startTime) + " ms");

      // when we are ready we move the index to the final destination and write
      // a done flag file we can use in shell scripts to identify the move is
      // done.

      new File(index, "done").createNewFile();

    } catch (Exception e) {
      throw new RuntimeException("Unable to write index", e);
    }
  }

  /**
   * creates a disctionary of words based on the input text.
   * 
   * @throws IOException
   */
  private String[] getWordList(String input) {
    try {
      Set<String> hashSet = new HashSet<String>();
      BufferedReader in = new BufferedReader(new FileReader(input));
      String str;
      while ((str = in.readLine()) != null) {
        String[] words = str.split(" ");
        for (String word : words) {
          hashSet.add(word);
        }
      }
      in.close();
      return hashSet.toArray(new String[hashSet.size()]);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read sample text", e);
    }
  }
}
