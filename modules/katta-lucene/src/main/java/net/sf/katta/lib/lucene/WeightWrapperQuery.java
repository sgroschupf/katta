package net.sf.katta.lib.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;

public class WeightWrapperQuery extends Query {
  private final Query _query;
  private final IndexSearcher _wrappedSearcher;

  public WeightWrapperQuery(Query query, IndexSearcher wrappedSearcher) {
    _query = query;
    _wrappedSearcher = wrappedSearcher;
  }


  public void setBoost(float b) {
    _query.setBoost(b);
  }

  public float getBoost() {
    return _query.getBoost();
  }

  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return _query.createWeight(_wrappedSearcher);
  }

  public String toString(String field) {
    return _query.toString(field);
  }

  public String toString() {
    return _query.toString();
  }

  public Query rewrite(IndexReader reader) throws IOException {
    Query rewrittenQuery = _query.rewrite(reader);
    if (rewrittenQuery != _query) {
      return new WeightWrapperQuery(rewrittenQuery, _wrappedSearcher);
    } else {
      return this;
    }
  }

  public void extractTerms(Set<Term> terms) {
    _query.extractTerms(terms);
  }
}
