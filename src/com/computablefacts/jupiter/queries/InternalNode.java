package com.computablefacts.jupiter.queries;

import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.DedupIterator;
import com.computablefacts.jupiter.storage.SynchronousIterator;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.nona.types.SpanSequence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * Internal (non-leaf) expression node class.
 *
 * See http://www.blackbeltcoder.com/Articles/data/easy-full-text-search-queries for details.
 */
@CheckReturnValue
final public class InternalNode extends AbstractNode {

  private static final Logger logger_ = LoggerFactory.getLogger(InternalNode.class);

  private eConjunctionTypes conjunction_;
  private AbstractNode child1_;
  private AbstractNode child2_;

  public InternalNode(eConjunctionTypes conjunction, AbstractNode child1, AbstractNode child2) {

    conjunction_ = Preconditions.checkNotNull(conjunction, "conjunction should not be null");

    child1_ = child1;
    child2_ = child2;
  }

  @Override
  public String toString() {
    return (grouped() ? "(" : "") + (child1_ == null ? "" : child1_.toString()) + " "
        + conjunction_.toString() + " " + (child2_ == null ? "" : child2_.toString())
        + (grouped() ? ")" : "");
  }

  @Override
  public Set<String> terms() {
    if (child1_ == null) {
      if (child2_ == null) {
        return Sets.newHashSet();
      }
      return child2_.terms();
    }
    if (child2_ == null) {
      return child1_.terms();
    }
    return Sets.union(child1_.terms(), child2_.terms());
  }

  @Override
  public long cardinality(DataStore dataStore, Scanners scanners, String dataset,
      Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset),
        "dataset should neither be null nor empty");

    // TODO : backport NOT implementation

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset)
          .add("conjunction", conjunction_).add("child1", child1_).add("child2", child2_)
          .formatInfo());
    }

    long cardChild1;
    long cardChild2;
    long cardinality;

    if (child1_ == null) {
      cardChild1 = 0;
    } else {
      cardChild1 = child1_.cardinality(dataStore, scanners, dataset, tokenizer);
    }

    if (child2_ == null) {
      cardChild2 = 0;
    } else {
      cardChild2 = child2_.cardinality(dataStore, scanners, dataset, tokenizer);
    }

    if (eConjunctionTypes.Or.equals(conjunction_)) {
      cardinality = cardChild1 + cardChild2;
    } else {
      cardinality = Math.min(cardChild1, cardChild2);
    }
    return cardinality;
  }

  @Override
  public Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilters<String> keepDocs, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset),
        "dataset should neither be null nor empty");

    // TODO : backport NOT implementation

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset)
          .add("conjunction", conjunction_).add("child1", child1_).add("child2", child2_)
          .formatInfo());
    }

    if (child1_ == null) {
      if (child2_ == null) {
        return Constants.ITERATOR_EMPTY;
      }
      return eConjunctionTypes.Or.equals(conjunction_)
          ? child2_.execute(dataStore, scanners, writers, dataset, keepDocs, tokenizer)
          : Constants.ITERATOR_EMPTY;
    }
    if (child2_ == null) {
      return eConjunctionTypes.Or.equals(conjunction_)
          ? child1_.execute(dataStore, scanners, writers, dataset, keepDocs, tokenizer)
          : Constants.ITERATOR_EMPTY;
    }

    Iterator<String> ids1 =
        child1_.execute(dataStore, scanners, writers, dataset, keepDocs, tokenizer);
    Iterator<String> ids2 =
        child2_.execute(dataStore, scanners, writers, dataset, keepDocs, tokenizer);

    if (eConjunctionTypes.Or.equals(conjunction_)) {

      // Advance both iterators synchronously. The assumption is that both iterators are sorted.
      // Hence, DataStore.Scanners should have been initialized with nbQueryThreads=1
      return new DedupIterator<>(
          Iterators.mergeSorted(Lists.newArrayList(ids1, ids2), String::compareTo));
    }

    // Advance both iterators synchronously. The assumption is that both iterators are sorted.
    // Hence, DataStore.Scanners should have been initialized with nbQueryThreads=1
    return new SynchronousIterator<>(ids1, ids2);
  }

  public eConjunctionTypes conjunction() {
    return conjunction_;
  }

  public void conjunction(eConjunctionTypes conjunction) {
    conjunction_ = conjunction;
  }

  public AbstractNode child1() {
    return child1_;
  }

  public void child1(AbstractNode child) {
    child1_ = child;
  }

  public AbstractNode child2() {
    return child2_;
  }

  public void child2(AbstractNode child) {
    child2_ = child;
  }

  public enum eConjunctionTypes {
    And, Or
  }
}
