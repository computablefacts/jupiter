/*
 * Copyright (c) 2011-2020 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
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
