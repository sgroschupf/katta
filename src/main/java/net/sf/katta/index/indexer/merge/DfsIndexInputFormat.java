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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class DfsIndexInputFormat extends FileInputFormat<Text, DocumentInformation> {

  @SuppressWarnings("hiding")
  protected final static Logger LOG = Logger.getLogger(DfsIndexInputFormat.class);

  public static final String DOCUMENT_INFORMATION = "document.duplicate.information.class";

  public RecordReader getRecordReader(final InputSplit inputSplit, final JobConf jobConf, Reporter reporter)
      throws IOException {
    IDocumentDuplicateInformation duplicateInformation = null;
    String className = jobConf.get(DOCUMENT_INFORMATION);
    try {
      Class<?> byName = jobConf.getClassByName(className);
      duplicateInformation = (IDocumentDuplicateInformation) byName.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("could not instantiate " + IDocumentDuplicateInformation.class.getName(), e);
    }
    duplicateInformation.setConf(jobConf);
    reporter.setStatus(((FileSplit) inputSplit).getPath().toString());
    return new DfsIndexRecordReader(jobConf, inputSplit, duplicateInformation);
  }

  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    FileSystem fileSystem = FileSystem.get(jobConf);
    Path[] indices = getZipIndices(jobConf, fileSystem);
    InputSplit[] splits = new InputSplit[indices.length];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new FileSplit(indices[i], 0, Integer.MAX_VALUE, jobConf);
    }
    return splits;
  }

  private Path[] getZipIndices(JobConf jobConf, FileSystem fileSystem) throws IOException {
    Path[] inputPaths = getInputPaths(jobConf);
    List<Path> zippedShards = new ArrayList<Path>();
    for (int i = 0; i < inputPaths.length; i++) {
      Path inputPath = inputPaths[i];
      getChilds(fileSystem, zippedShards, inputPath);
    }
    if (zippedShards.isEmpty()) {
      throw new IllegalStateException("could not find any zipped shard in: " + Arrays.asList(inputPaths));
    }
    return zippedShards.toArray(new Path[zippedShards.size()]);
  }

  private void getChilds(final FileSystem fileSystem, List<Path> list, Path inputPath) throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(inputPath, new PathFilter() {
      public boolean accept(Path path) {
        boolean ret = false;
        try {
          boolean isFile = fileSystem.isFile(path);
          boolean isZip = path.getName().endsWith(".zip");
          ret = !isFile || isZip;
        } catch (IOException e) {
          LOG.warn("can not get child directory", e);
        }
        return ret;
      }
    });
    for (int j = 0; j < fileStatuses.length; j++) {
      FileStatus fileStatuse = fileStatuses[j];
      Path path = fileStatuse.getPath();
      if (!fileSystem.isFile(path)) {
        getChilds(fileSystem, list, path);
      } else {
        list.add(path);
      }
    }
  }

}
