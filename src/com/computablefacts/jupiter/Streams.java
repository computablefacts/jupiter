package com.computablefacts.jupiter;

import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Streams {

  public static <T> void forEach(Stream<T> stream, BiConsumer<T, Breaker> consumer) {

    Spliterator<T> spliterator = stream.spliterator();
    Breaker breaker = new Breaker();
    @Var
    boolean hadNext = true;

    while (hadNext && !breaker.shouldBreak()) {
      hadNext = spliterator.tryAdvance(elem -> consumer.accept(elem, breaker));
    }
  }

  public static class Breaker {

    private boolean shouldBreak_ = false;

    public void stop() {
      shouldBreak_ = true;
    }

    public boolean shouldBreak() {
      return shouldBreak_;
    }
  }
}
