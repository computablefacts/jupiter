package com.computablefacts.jupiter.storage;

import java.util.Iterator;

import org.apache.accumulo.core.util.PeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.errorprone.annotations.Var;

/**
 * Returns elements that are presents in both iterators. Duplicate elements are returned only once.
 * Both iterators MUST BE sorted.
 *
 * @param <T> type of the entries returned by both the source and this iterators.
 */
final public class SynchronousIterator<T extends Comparable<T>> extends AbstractIterator<T> {

  private final PeekingIterator<T> iter1_;
  private final PeekingIterator<T> iter2_;

  public SynchronousIterator(Iterator<T> iter1, Iterator<T> iter2) {
    iter1_ = new PeekingIterator<>(Preconditions.checkNotNull(iter1, "iter1 should not be null"));
    iter2_ = new PeekingIterator<>(Preconditions.checkNotNull(iter2, "iter2 should not be null"));
  }

  @Override
  protected T computeNext() {

    if (!iter1_.hasNext() || !iter2_.hasNext()) {
      return endOfData();
    }

    @Var
    T cur = null;

    while (iter1_.hasNext() && iter2_.hasNext()) {

      cur = iter1_.next();

      while (iter2_.hasNext() && iter2_.peek().compareTo(cur) < 0) {
        iter2_.next();
      }

      if (!iter2_.hasNext()) {
        return endOfData();
      }

      int cmp = iter2_.peek().compareTo(cur);

      if (cmp == 0) {
        while (iter1_.hasNext() && iter1_.peek().equals(cur)) {
          cur = iter1_.next();
        }
        while (iter2_.hasNext() && iter2_.peek().equals(cur)) {
          cur = iter2_.next();
        }
        return cur;
      }
    }
    return endOfData();
  }
}
