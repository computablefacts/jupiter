package com.computablefacts.jupiter.storage;

import java.util.Iterator;

import org.apache.accumulo.core.util.PeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * Remove consecutive duplicates. The source iterator MUST BE sorted.
 *
 * @param <T> type of the entries returned by both the source and this iterators.
 */
@CheckReturnValue
final public class DedupIterator<T> extends AbstractIterator<T> {

  private final PeekingIterator<T> iterator_;

  public DedupIterator(Iterator<T> iterator) {
    iterator_ =
        new PeekingIterator<>(Preconditions.checkNotNull(iterator, "iterator should not be null"));
  }

  @Override
  protected T computeNext() {

    if (!iterator_.hasNext()) {
      return endOfData();
    }

    @Var
    T curr = iterator_.next();

    while (iterator_.hasNext() && curr.equals(iterator_.peek())) {
      curr = iterator_.next();
    }
    return curr;
  }
}
