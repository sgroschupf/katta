package net.sf.katta.lib.lucene;

import org.apache.lucene.search.IndexSearcher;

import java.util.Map;

public class GlobalDfSearcher2 extends IndexSearcher {
  public GlobalDfSearcher2(Map<TermWritable, Integer> dfMap, int maxDoc) {
    super(new GlobalDfReader(dfMap, maxDoc));

  }
}
