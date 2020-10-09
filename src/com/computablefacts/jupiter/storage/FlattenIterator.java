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
import java.util.List;
import java.util.function.Function;

import org.apache.accumulo.core.util.PeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * Flatten an iterator. Optionally map the iterator entries at the same time.
 * 
 * @param <T> type of the entries returned by the source iterator.
 * @param <U> type of the entries returned by this iterator.
 */
@CheckReturnValue
final public class FlattenIterator<T, U> extends AbstractIterator<U> {

  private final PeekingIterator<T> iterator_;
  private final Function<T, List<U>> flatten_;

  private List<U> list_;
  private int index_;

  public FlattenIterator(Iterator<T> iterator, Function<T, List<U>> flatten) {
    iterator_ =
        new PeekingIterator<>(Preconditions.checkNotNull(iterator, "iterator should not be null"));
    flatten_ = Preconditions.checkNotNull(flatten, "flatten should not be null");
  }

  @Override
  protected U computeNext() {

    if (list_ == null || index_ >= list_.size()) {

      list_ = null;
      index_ = 0;

      while (iterator_.hasNext() && (list_ == null || list_.isEmpty())) {

        List<U> list = flatten_.apply(iterator_.next());

        if (list != null && !list.isEmpty()) {
          list_ = list;
        }
      }

      if (list_ == null || list_.isEmpty()) {
        return endOfData();
      }
    }
    return list_.get(index_++);
  }
}
