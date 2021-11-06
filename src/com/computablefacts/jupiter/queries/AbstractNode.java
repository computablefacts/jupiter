package com.computablefacts.jupiter.queries;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import com.computablefacts.asterix.View;
import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.nona.types.SpanSequence;
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

  final public View<Map.Entry<String, String>> execute(DataStore dataStore,
      Authorizations authorizations, String dataset) {
    return execute(dataStore, authorizations, dataset, null, null).map(t -> {
      int index = t.indexOf(SEPARATOR_NUL);
      return new AbstractMap.SimpleEntry<>(t.substring(0, index), t.substring(index + 1));
    });
  }

  final public View<Map.Entry<String, String>> execute(DataStore dataStore,
      Authorizations authorizations, String dataset, BloomFilters<String> expectedDocsIds) {
    return execute(dataStore, authorizations, dataset, expectedDocsIds, null).map(t -> {
      int index = t.indexOf(SEPARATOR_NUL);
      return new AbstractMap.SimpleEntry<>(t.substring(0, index), t.substring(index + 1));
    });
  }

  @Deprecated
  public abstract Set<String> terms();

  public abstract long cardinality(DataStore dataStore, Authorizations authorizations,
      String dataset, Function<String, SpanSequence> tokenizer);

  public abstract View<String> execute(DataStore dataStore, Authorizations authorizations,
      String dataset, BloomFilters<String> expectedDocsIds,
      Function<String, SpanSequence> tokenizer);
}
