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
  private final Weight _weight;

  public WeightWrapperQuery(Query query, Weight weight) {
    _query = query;
    _weight = weight;
  }


  public void setBoost(float b) {
    _query.setBoost(b);
  }

  public float getBoost() {
    return _query.getBoost();
  }

  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return _weight;
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
      return new WeightWrapperQuery(rewrittenQuery, _weight);
    } else {
      return this;
    }
  }

  public void extractTerms(Set<Term> terms) {
    _query.extractTerms(terms);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    WeightWrapperQuery that = (WeightWrapperQuery) o;

    if (!_query.equals(that._query)) {
      return false;
    }
    if (!_weight.equals(that._weight)) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + _query.hashCode();
    result = 31 * result + _weight.hashCode();
    return result;
  }
}
