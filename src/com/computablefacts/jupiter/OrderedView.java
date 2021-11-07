package com.computablefacts.jupiter;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.accumulo.core.client.Scanner;

import com.computablefacts.asterix.View;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class OrderedView<T> extends View<T> {

  private final Scanner scanner_;
  private boolean isClosed_ = false;

  public OrderedView(Scanner scanner, Function<Scanner, Iterator<T>> newIterator) {
    super(newIterator.apply(scanner));
    scanner_ = scanner;
  }

  @Override
  public T computeNext() {
    if (isClosed_) {
      return endOfData();
    }
    T t = iterator_.hasNext() ? iterator_.next() : null;
    if (!iterator_.hasNext()) {
      scanner_.close();
      isClosed_ = true;
    }
    return t == null ? endOfData() : t;
  }

  @Override
  public void close() {
    if (!isClosed_ && scanner_ != null) {
      scanner_.close();
      isClosed_ = true;
    }
  }
}
