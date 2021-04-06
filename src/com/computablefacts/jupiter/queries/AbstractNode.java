package com.computablefacts.jupiter.queries;

import java.util.Iterator;
import java.util.Set;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
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

  final public long count(DataStore dataStore, Scanners scanners, String dataset) {
    return count(dataStore, scanners, dataset, null);
  }

  final public Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset) {
    return execute(dataStore, scanners, writers, dataset, null, null);
  }

  final public Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilters<String> keepDocs) {
    return execute(dataStore, scanners, writers, dataset, keepDocs, null);
  }

  @Deprecated
  public abstract Set<String> terms();

  public abstract long count(DataStore dataStore, Scanners scanners, String dataset,
      Function<String, SpanSequence> tokenizer);

  public abstract Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilters<String> keepDocs, Function<String, SpanSequence> tokenizer);
}
