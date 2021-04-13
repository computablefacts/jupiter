package com.computablefacts.jupiter.storage;

import java.util.Iterator;

import org.apache.accumulo.core.util.PeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * Returns elements that are presents in the first iterator but not in the second one. Duplicate
 * elements are returned only once. Both iterators MUST BE sorted.
 *
 * @param <T> type of the entries returned by both the source and this iterators.
 */
public class DifferenceIterator<T extends Comparable<T>> extends AbstractIterator<T> {

  private final PeekingIterator<T> iter1_;
  private final PeekingIterator<T> iter2_;
  private T cur2_;

  public DifferenceIterator(Iterator<T> iter1, Iterator<T> iter2) {
    iter1_ = new PeekingIterator<>(Preconditions.checkNotNull(iter1, "iter1 should not be null"));
    iter2_ = new PeekingIterator<>(Preconditions.checkNotNull(iter2, "iter2 should not be null"));
  }

  @Override
  protected T computeNext() {

    while (iter1_.hasNext()) {

      T cur1 = iter1_.next();

      while (iter2_.hasNext() && (cur2_ == null || cur1.compareTo(cur2_) > 0)) {
        cur2_ = iter2_.next();
      }

      if (cur2_ != null && cur1.compareTo(cur2_) > 0) {
        cur2_ = iter2_.next();
        return cur1;
      }
      if (cur2_ == null || cur1.compareTo(cur2_) < 0) {
        return cur1;
      }
    }
    return endOfData();
  }
}
