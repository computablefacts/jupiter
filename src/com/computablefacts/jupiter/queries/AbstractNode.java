package com.computablefacts.jupiter.queries;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import com.computablefacts.asterix.SpanSequence;
import com.computablefacts.asterix.View;
import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.google.common.base.Function;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * Common interface for expression nodes.
 *
 * See http://www.blackbeltcoder.com/Articles/data/easy-full-text-search-queries for details.
 */
@CheckReturnValue
public abstract class AbstractNode {

  private boolean exclude_ = false;

  public AbstractNode() {
    super();
  }

  /**
   * Indicates this term (or both child terms) should be excluded from the results.
   */
  final public boolean exclude() {
    return this.exclude_;
  }

  final public void exclude(boolean exclude) {
    this.exclude_ = exclude;
  }

  final public long cardinality(DataStore dataStore, Authorizations authorizations,
      String dataset) {
    return cardinality(dataStore, authorizations, dataset, null);
  }

  final public View<String> execute(DataStore dataStore, Authorizations authorizations,
      String dataset) {
    return execute(dataStore, authorizations, dataset, null, null);
  }

  final public View<String> execute(DataStore dataStore, Authorizations authorizations,
      String dataset, BloomFilters<String> expectedDocsIds) {
    return execute(dataStore, authorizations, dataset, expectedDocsIds, null);
  }

  @Deprecated
  public abstract Set<String> terms();

  public abstract long cardinality(DataStore dataStore, Authorizations authorizations,
      String dataset, Function<String, SpanSequence> tokenizer);

  public abstract View<String> execute(DataStore dataStore, Authorizations authorizations,
      String dataset, BloomFilters<String> expectedDocsIds,
      Function<String, SpanSequence> tokenizer);
}
