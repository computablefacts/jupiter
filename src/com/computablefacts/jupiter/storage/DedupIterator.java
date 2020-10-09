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
