package net.sf.katta.lib.lucene.query;

import net.sf.katta.lib.lucene.TermWritable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class TermQueryWritable implements ILuceneQueryAndFilterWritable, Serializable {
  private TermWritable _termWritable;

  private TermQueryWritable() { }

  public TermQueryWritable(TermWritable termWritable) {
    this._termWritable = termWritable;
  }

  public TermQueryWritable(String field, String term) {
    this._termWritable = new TermWritable(field, term);
  }

  public Query getQuery() {
    return new TermQuery(new Term(_termWritable.getField(), _termWritable.getTerm()));
  }

  public Filter getFilter() {
    return null;
  }

  public void write(DataOutput out) throws IOException {
    _termWritable.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    _termWritable = new TermWritable();
    _termWritable.readFields(in);
  }
}
