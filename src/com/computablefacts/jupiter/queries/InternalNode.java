package com.computablefacts.jupiter.queries;

import com.computablefacts.asterix.SpanSequence;
import com.computablefacts.asterix.View;
import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal (non-leaf) expression node class.
 * <p>
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
    return (exclude() ? "Not(" : "(") + (child1_ == null ? "" : child1_.toString()) + " " + conjunction_.toString()
        + " " + (child2_ == null ? "" : child2_.toString()) + ")";
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
  public long cardinality(DataStore dataStore, Authorizations authorizations, String dataset,
      Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("dataset", dataset).add("conjunction", conjunction_).add("child1", child1_)
              .add("child2", child2_).formatDebug());
    }

    long cardChild1;
    long cardChild2;
    long cardinality;

    if (child1_ == null) {
      cardChild1 = 0;
    } else {
      cardChild1 = child1_.cardinality(dataStore, authorizations, dataset, tokenizer);
    }

    if (child2_ == null) {
      cardChild2 = 0;
    } else {
      cardChild2 = child2_.cardinality(dataStore, authorizations, dataset, tokenizer);
    }

    if (child1_ != null && child2_ != null) {

      // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B, NOT A OR B, A OR NOT B,
      // NOT A AND NOT B, NOT A OR NOT B}
      if (child1_.exclude() && child2_.exclude()) {
        return 0; // (NOT A AND NOT B) or (NOT A OR NOT B)
      }

      // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B, NOT A OR B, A OR NOT B}
      if (eConjunctionTypes.Or.equals(conjunction_) && (child1_.exclude() || child2_.exclude())) {
        if (child1_.exclude()) {
          return cardChild2; // NOT A OR B
        }
        return cardChild1; // A OR NOT B
      }

      // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B}
      if (eConjunctionTypes.And.equals(conjunction_) && (child1_.exclude() || child2_.exclude())) {
        if (child1_.exclude()) {
          return cardChild2; // NOT A AND B -> should be Math.min(cardChild2, #entries - cardChild1)
        }
        return cardChild1; // A AND NOT B -> should be Math.min(cardChild1, #entries - cardChild2)
      }
    }

    // Here, the query is in {A OR B, A AND B}
    if (eConjunctionTypes.Or.equals(conjunction_)) {
      cardinality = cardChild1 + cardChild2;
    } else {
      cardinality = Math.min(cardChild1, cardChild2);
    }
    return cardinality;
  }

  @Override
  public View<String> execute(DataStore dataStore, Authorizations authorizations, String dataset,
      BloomFilters<String> expectedDocsIds, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("dataset", dataset).add("conjunction", conjunction_).add("child1", child1_)
              .add("child2", child2_).formatDebug());
    }

    if (child1_ == null) {
      if (child2_ == null) {
        return View.of();
      }
      if (child2_.exclude()) { // (NULL AND/OR NOT B) is not a valid construct
        if (logger_.isErrorEnabled()) {
          logger_.error(LogFormatter.create(true).add("dataset", dataset).add("query", toString())
              .message("ill-formed query : (NULL AND/OR NOT B)").formatError());
        }
        return View.of();
      }
      return eConjunctionTypes.Or.equals(conjunction_) ? child2_.execute(dataStore, authorizations, dataset,
          expectedDocsIds, tokenizer) : View.of();
    }
    if (child2_ == null) {
      if (child1_.exclude()) { // (NOT A AND/OR NULL) is not a valid construct
        if (logger_.isErrorEnabled()) {
          logger_.error(LogFormatter.create(true).add("dataset", dataset).add("query", toString())
              .message("ill-formed query : (NOT A AND/OR NULL)").formatError());
        }
        return View.of();
      }
      return eConjunctionTypes.Or.equals(conjunction_) ? child1_.execute(dataStore, authorizations, dataset,
          expectedDocsIds, tokenizer) : View.of();
    }

    // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B, NOT A OR B, A OR NOT B, NOT
    // A AND NOT B, NOT A OR NOT B}
    if (child1_.exclude() && child2_.exclude()) {
      if (logger_.isErrorEnabled()) {
        logger_.error(LogFormatter.create(true).add("dataset", dataset).add("query", toString())
            .message("ill-formed query : (NOT A AND/OR NOT B)").formatError());
      }
      return View.of(); // (NOT A AND NOT B) or (NOT A OR NOT B)
    }

    // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B, NOT A OR B, A OR NOT B}
    if (eConjunctionTypes.Or.equals(conjunction_) && (child1_.exclude() || child2_.exclude())) {
      if (logger_.isErrorEnabled()) {
        logger_.error(LogFormatter.create(true).add("dataset", dataset).add("query", toString())
            .message("ill-formed query : (A OR NOT B) or (NOT A OR B)").formatError());
      }
      return View.of();
    }

    View<String> ids1 = child1_.execute(dataStore, authorizations, dataset, expectedDocsIds, tokenizer);
    View<String> ids2 = child2_.execute(dataStore, authorizations, dataset, expectedDocsIds, tokenizer);

    // Here, the query is in {A OR B, A AND B, NOT A AND B, A AND NOT B}
    if (eConjunctionTypes.And.equals(conjunction_) && (child1_.exclude() || child2_.exclude())) {
      if (child1_.exclude()) {
        return ids2.diffSorted(ids1); // NOT A AND B
      }
      return ids1.diffSorted(ids2); // A AND NOT B
    }

    // Here, the query is in {A OR B, A AND B}
    if (eConjunctionTypes.Or.equals(conjunction_)) {

      List<View<String>> list = new ArrayList<>();
      list.add(ids2);

      // Advance both iterators synchronously. The assumption is that both iterators are sorted.
      return ids1.mergeSorted(list, String::compareTo);
    }

    // Advance both iterators synchronously. The assumption is that both iterators are sorted.
    return ids1.intersectSorted(ids2);
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
