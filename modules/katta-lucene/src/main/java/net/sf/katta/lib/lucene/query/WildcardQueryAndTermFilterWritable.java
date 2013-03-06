package net.sf.katta.lib.lucene.query;

import net.sf.katta.lib.lucene.TermWritable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WildcardQueryAndTermFilterWritable implements ILuceneQueryAndFilterWritable {
  private TermWritable _termWritable;
  private TermWritable _filterTermWritable;

  private WildcardQueryAndTermFilterWritable() { }

  public WildcardQueryAndTermFilterWritable(TermWritable termWritable, TermWritable filterTermWritable) {
    this._termWritable = termWritable;
    this._filterTermWritable = filterTermWritable;
  }

  public WildcardQueryAndTermFilterWritable(String field, String term, String filterField, String filterTerm) {
    this._termWritable = new TermWritable(field, term);
    this._filterTermWritable = new TermWritable(filterField, term);
  }

  public Query getQuery() {
    return new WildcardQuery(new Term(_termWritable.getField(), _termWritable.getTerm()));
  }

  public Filter getFilter() {
    return new QueryWrapperFilter(new TermQuery(new Term(_filterTermWritable.getField(), _filterTermWritable.getTerm())));
  }

  public void write(DataOutput out) throws IOException {
    _termWritable.write(out);
    _filterTermWritable.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    _termWritable = new TermWritable();
    _termWritable.readFields(in);
    _filterTermWritable = new TermWritable();
    _filterTermWritable.readFields(in);
  }
}