package com.computablefacts.jupiter.storage.termstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.errorprone.annotations.Var;

/**
 * Assumption : the source iterator MUST BE sorted by terms.
 */
final class GroupByTermIterator<T extends HasTerm> extends AbstractIterator<Pair<String, List<T>>> {

  private final PeekingIterator<T> iterator_;

  public GroupByTermIterator(Iterator<T> iterator) {
    iterator_ =
        new PeekingIterator<>(Preconditions.checkNotNull(iterator, "iterator should not be null"));
  }

  @Override
  protected Pair<String, List<T>> computeNext() {

    if (!iterator_.hasNext()) {
      return endOfData();
    }

    @Var
    String row = null;
    List<T> list = new ArrayList<>();

    while (iterator_.hasNext() && (row == null || row.equals(iterator_.peek().term()))) {
      T term = iterator_.next();
      row = term.term();
      list.add(term);
    }
    return new Pair<>(row, list);
  }
}
