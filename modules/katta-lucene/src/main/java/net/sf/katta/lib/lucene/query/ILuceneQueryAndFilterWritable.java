package net.sf.katta.lib.lucene.query;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

public interface ILuceneQueryAndFilterWritable extends Writable {
  public Query getQuery();
  public Filter getFilter();
}
