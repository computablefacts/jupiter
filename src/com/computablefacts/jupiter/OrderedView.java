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
    if (!hasNext()) {
      scanner_.close();
      isClosed_ = true;
      return endOfData();
    }
    return super.computeNext();
  }

  @Override
  public void close() {
    if (!isClosed_ && scanner_ != null) {
      scanner_.close();
      isClosed_ = true;
    }
    super.close();
  }
}
