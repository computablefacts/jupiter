package com.computablefacts.jupiter;

import com.computablefacts.asterix.View;
import com.google.errorprone.annotations.CheckReturnValue;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.accumulo.core.client.BatchScanner;

@CheckReturnValue
final public class UnorderedView<T> extends View<T> {

  private final BatchScanner scanner_;
  private boolean isClosed_ = false;

  public UnorderedView(BatchScanner scanner, Function<BatchScanner, Iterator<T>> newIterator) {
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
