package com.computablefacts.jupiter.queries;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.nona.types.SpanSequence;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
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

  final public long cardinality(DataStore dataStore, Scanners scanners, String dataset) {
    return cardinality(dataStore, scanners, dataset, null);
  }

  final public Iterator<Map.Entry<String, String>> execute(DataStore dataStore, Scanners scanners,
      Writers writers, String dataset) {
    return Iterators.transform(execute(dataStore, scanners, writers, dataset, null, null), t -> {
      int index = t.indexOf(SEPARATOR_NUL);
      return new AbstractMap.SimpleEntry<>(t.substring(0, index), t.substring(index + 1));
    });
  }

  final public Iterator<Map.Entry<String, String>> execute(DataStore dataStore, Scanners scanners,
      Writers writers, String dataset, BloomFilters<String> docsIds) {
    return Iterators.transform(execute(dataStore, scanners, writers, dataset, docsIds, null), t -> {
      int index = t.indexOf(SEPARATOR_NUL);
      return new AbstractMap.SimpleEntry<>(t.substring(0, index), t.substring(index + 1));
    });
  }

  @Deprecated
  public abstract Set<String> terms();

  public abstract long cardinality(DataStore dataStore, Scanners scanners, String dataset,
      Function<String, SpanSequence> tokenizer);

  public abstract Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilters<String> docsIds, Function<String, SpanSequence> tokenizer);
}
