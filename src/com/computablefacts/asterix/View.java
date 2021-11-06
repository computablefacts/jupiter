package com.computablefacts.asterix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
public class View<T> extends AbstractIterator<T> implements AutoCloseable {

  private static final View<?> EMPTY_VIEW = new View<>(Collections.emptyIterator());
  private static final Object END_MARKER = new Object();
  private static final Object NULL_MARKER = new Object();
  private static final ExecutorService defaultExecutor_ = Executors.newCachedThreadPool();

  protected final Iterator<T> stream_;

  protected View(Iterator<T> stream) {
    stream_ = Preconditions.checkNotNull(stream, "stream should not be null");
  }

  public static <T> View<T> of() {
    return (View<T>) EMPTY_VIEW;
  }

  public static <T> View<T> of(Stream<T> stream) {

    Preconditions.checkNotNull(stream, "stream should not be null");

    return new View<>(stream.iterator());
  }

  public static <T> View<T> of(Iterator<T> iterator) {
    return new View<>(iterator);
  }

  public static <T> View<T> of(List<T> list) {

    Preconditions.checkNotNull(list, "list should not be null");

    return new View<>(list.iterator());
  }

  public static <T> View<T> of(Set<T> set) {

    Preconditions.checkNotNull(set, "set should not be null");

    return new View<>(set.iterator());
  }

  public static <T> View<T> of(Enumeration<T> enumeration) {
    return new View<>(Iterators.forEnumeration(enumeration));
  }

  public static <K, V> View<Map.Entry<K, V>> of(Map<K, V> map) {

    Preconditions.checkNotNull(map, "map should not be null");

    return of(map.entrySet());
  }

  public static <T> View<T> of(ResultSet rs, Function<ResultSet, T> fn) {

    Preconditions.checkNotNull(rs, "rs should not be null");
    Preconditions.checkNotNull(fn, "fn should not be null");

    return of(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {
        try {
          if (rs.next()) {
            return fn.apply(rs);
          }
        } catch (SQLException e) {
          // TODO
        }
        return endOfData();
      }
    });
  }

  /**
   * Returns an infinite sequence of the same object.
   *
   * @param object the object to return.
   * @param <T>
   * @return an infinite sequence of the same object.
   */
  public static <T> View<T> repeat(T object) {
    return iterate(object, s -> s);
  }

  /**
   * Returns a sequence of integers in {@code begin} included and {@code end} excluded.
   *
   * @param begin the beginning of the sequence (included).
   * @param end the end of the sequence (excluded).
   * @return a sequence of consecutive integers.
   */
  public static View<Integer> range(int begin, int end) {

    Preconditions.checkArgument(begin >= 0, "begin must be >= 0");
    Preconditions.checkArgument(end >= begin, "end must be >= begin");

    return iterate(begin, x -> x + 1).take(end - begin);
  }

  /**
   * Returns a sequence of objects.
   *
   * @param seed the first value.
   * @param next a function that compute the next value from the previous one.
   * @param <T>
   * @return a sequence of objects.
   */
  public static <T> View<T> iterate(T seed, Function<T, T> next) {
    return of(new AbstractSequentialIterator<T>(seed) {

      @Nullable
      @Override
      protected T computeNext(T previous) {
        return next.apply(previous);
      }
    });
  }

  @Override
  public T computeNext() {
    return stream_.hasNext() ? stream_.next() : endOfData();
  }

  @Override
  public void close() {
    if (stream_ instanceof AutoCloseable) {
      try {
        ((AutoCloseable) stream_).close();
      } catch (Exception e) {
        // TODO
      }
    }
  }

  @Override
  protected void finalize() {
    close();
  }

  /**
   * Accumulates the view elements into a new {@link List}.
   *
   * @return a {@link List}.
   */
  public List<T> toList() {
    return Lists.newArrayList(this);
  }

  /**
   * Accumulates the view elements into a new sorted {@link List}.
   *
   * @return a {@link List}.
   */
  public List<T> toSortedList(Comparator<T> comparator) {
    List<T> list = toList();
    list.sort(comparator);
    return list;
  }

  /**
   * Accumulates the view elements into a new {@link Set}.
   *
   * @return a {@link Set}.
   */
  public Set<T> toSet() {
    return Sets.newHashSet(this);
  }

  /**
   * Returns a {@link Map} where keys are the result of a function applied to each element of the
   * view and values are lists of elements corresponding to each key.
   *
   * @param fn the function to apply.
   * @param <U>
   * @return a {@link Map}.
   */
  public <U> Map<U, List<T>> groupAll(Function<T, U> fn) {

    Preconditions.checkNotNull(fn, "fn should not be null");

    Map<U, List<T>> groups = new HashMap<>();

    while (hasNext()) {

      T value = next();
      U key = fn.apply(value);

      if (!groups.containsKey(key)) {
        groups.put(key, new ArrayList<>());
      }
      groups.get(key).add(value);
    }
    return groups;
  }

  /**
   * Returns a {@link Map} where keys are the result of a function applied to each element of the
   * view and values are sets of elements corresponding to each key.
   *
   * @param fn the function to apply.
   * @param <U>
   * @return a {@link Map}.
   */
  public <U> Map<U, Set<T>> groupDistinct(Function<T, U> fn) {

    Preconditions.checkNotNull(fn, "fn should not be null");

    Map<U, Set<T>> groups = new HashMap<>();

    while (hasNext()) {

      T value = next();
      U key = fn.apply(value);

      if (!groups.containsKey(key)) {
        groups.put(key, new HashSet<>());
      }
      groups.get(key).add(value);
    }
    return groups;
  }

  /**
   * Assemble two lists into one by combining the elements of the same index.
   *
   * @param view the other view.
   * @param <U>
   * @return a new {@link View} of the combined views.
   */
  public <U> View<Map.Entry<T, U>> zip(View<U> view) {

    Preconditions.checkNotNull(view, "view should not be null");

    View<T> self = this;
    return new View<>(new AbstractIterator<Map.Entry<T, U>>() {

      @Override
      protected Map.Entry<T, U> computeNext() {
        if (self.hasNext() && view.hasNext()) {
          return new AbstractMap.SimpleImmutableEntry<>(self.next(), view.next());
        }
        return endOfData();
      }
    });
  }

  /**
   * Makes two lists out of one by "deconstructing" the elements.
   *
   * @param fn a function that maps a single element to a tuple.
   * @param <U>
   * @param <V>
   * @return a {@link Map.Entry}.
   */
  public <U, V> Map.Entry<List<U>, List<V>> unzip(Function<T, Map.Entry<U, V>> fn) {

    Preconditions.checkNotNull(fn, "fn should not be null");

    return map(fn).reduce(
        new AbstractMap.SimpleImmutableEntry<>(new ArrayList<>(), new ArrayList<>()),
        (carry, e) -> {
          carry.getKey().add(e.getKey());
          carry.getValue().add(e.getValue());
          return carry;
        });
  }

  /**
   * Adds an element to the end of the view.
   *
   * @param element the element to add.
   * @return a new {@link View}.
   */
  public View<T> append(T element) {
    return new View<>(Iterators.concat(this, Lists.newArrayList(element).iterator()));
  }

  /**
   * Adds an element to the beginning of the view.
   *
   * @param element the element to add.
   * @return a new {@link View}.
   */
  public View<T> prepend(T element) {
    return new View<>(Iterators.concat(Lists.newArrayList(element).iterator(), this));
  }

  /**
   * Returns a view where all elements are indexed by there position in the underlying stream of
   * values.
   * 
   * @return a new {@link View}.
   */
  public View<Map.Entry<Integer, T>> index() {

    View<T> self = this;
    return new View<>(new AbstractIterator<Map.Entry<Integer, T>>() {

      private int index_ = 0;

      @Override
      protected Map.Entry<Integer, T> computeNext() {
        return self.hasNext() ? new AbstractMap.SimpleImmutableEntry<>(++index_, self.next())
            : endOfData();
      }
    });
  }

  /**
   * Returns whether any elements of this view match the provided predicate.
   *
   * @param predicate the predicate to satisfy.
   * @return true if one or more elements returned by this view satisfy the given predicate.
   */
  public boolean contains(Predicate<? super T> predicate) {
    return anyMatch(predicate);
  }

  /**
   * Returns whether any elements of this view match the provided predicate.
   *
   * @param predicate the predicate to satisfy.
   * @return true if one or more elements returned by this view satisfy the given predicate.
   */
  public boolean anyMatch(Predicate<? super T> predicate) {
    return Iterators.any(this, predicate::test);
  }

  /**
   * Returns whether all elements of this view match the provided predicate.
   *
   * @param predicate the predicate to satisfy.
   * @return true if every element returned by this view satisfies the given predicate. If the view
   *         is empty, true is returned.
   */
  public boolean allMatch(Predicate<? super T> predicate) {
    return Iterators.all(this, predicate::test);
  }

  /**
   * Returns an {@link Optional} containing the first element of this view that satisfies the
   * provided predicate.
   *
   * @param predicate the predicate to satisfy.
   * @return an {@link Optional}.
   */
  public Optional<T> findFirst(Predicate<? super T> predicate) {
    return Iterators.tryFind(this, predicate::test).toJavaUtil();
  }

  /**
   * Returns a {@link View} containing all the elements of this view that satisfy the provided
   * predicate.
   *
   * @param predicate the predicate to satisfy.
   * @return a new {@link View}.
   */
  public View<T> findAll(Predicate<? super T> predicate) {
    return filter(predicate);
  }

  /**
   * Performs the given action for each remaining element of the view until all elements have been
   * processed or the caller stopped the enumeration.
   *
   * @param consumer the action to be performed for each element.
   */
  public void forEachRemaining(BiConsumer<? super T, Breaker> consumer) {

    Preconditions.checkNotNull(consumer, "consumer should not be null");

    Breaker breaker = new Breaker();

    while (!breaker.shouldBreak() && hasNext()) {
      consumer.accept(next(), breaker);
    }
  }

  /**
   * Reduce the view to a single value using a given operation.
   * 
   * @param carry the neutral element.
   * @param operation the operation to apply.
   * @param <U>
   * @return a single value.
   */
  public <U> U reduce(@Var U carry, BiFunction<U, T, U> operation) {

    Preconditions.checkNotNull(carry, "carry should not be null");
    Preconditions.checkNotNull(operation, "operation should not be null");

    while (hasNext()) {
      carry = operation.apply(carry, next());
    }
    return carry;
  }

  /**
   * Returns the first {@code n} elements of a view.
   *
   * @param n the number of elements to keep.
   * @return the first {@code n} elements of the {@link View}.
   */
  public View<T> take(long n) {

    Preconditions.checkArgument(n >= 0, "n must be >= 0");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      private long taken_ = 0;

      @Override
      protected T computeNext() {
        if (self.hasNext() && taken_ < n) {
          taken_++;
          return self.next();
        }
        return endOfData();
      }
    });
  }

  /**
   * Returns a view containing all starting elements as long as a condition is matched.
   *
   * @param predicate the condition to match.
   * @return a {@link View} of the matching elements.
   */
  public View<T> takeWhile(Predicate<? super T> predicate) {

    Preconditions.checkNotNull(predicate, "predicate should not be null");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {
        if (self.hasNext()) {
          T e = self.next();
          if (predicate.test(e)) {
            return e;
          }
        }
        return endOfData();
      }
    });
  }

  /**
   * Returns a view containing all starting elements as long as a condition is not matched.
   *
   * @param predicate the condition to match.
   * @return a {@link View} of the matching elements.
   */
  public View<T> takeUntil(Predicate<? super T> predicate) {

    Preconditions.checkNotNull(predicate, "predicate should not be null");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {
        if (self.hasNext()) {
          T e = self.next();
          if (!predicate.test(e)) {
            return e;
          }
        }
        return endOfData();
      }
    });
  }

  /**
   * Returns a view with the first {@code n} elements removed.
   *
   * @param n the number of elements to remove.
   * @return a {@link View} with the first {@code n} elements removed.
   */
  public View<T> drop(long n) {

    Preconditions.checkArgument(n >= 0, "n must be >= 0");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      private long dropped_ = 0;

      @Override
      protected T computeNext() {
        while (self.hasNext() && dropped_ < n) {
          dropped_++;
          self.next();
        }
        return self.hasNext() ? self.next() : endOfData();
      }
    });
  }

  /**
   * Returns a view with the front elements removed as long as they satisfy a condition.
   * 
   * @param predicate the condition to satisfy.
   * @return a {@link View} with the front elements removed.
   */
  public View<T> dropWhile(Predicate<? super T> predicate) {

    Preconditions.checkNotNull(predicate, "predicate should not be null");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      private boolean dropped_ = false;

      @Override
      protected T computeNext() {
        if (!dropped_) {
          while (self.hasNext()) {
            T e = self.next();
            if (!predicate.test(e)) {
              dropped_ = true;
              return e;
            }
          }
          dropped_ = true;
        }
        return self.hasNext() ? self.next() : endOfData();
      }
    });
  }

  /**
   * Returns a view with the front elements removed as long as they do not satisfy a condition.
   *
   * @param predicate the condition to satisfy.
   * @return a {@link View} with the front elements removed.
   */
  public View<T> dropUntil(Predicate<? super T> predicate) {

    Preconditions.checkNotNull(predicate, "predicate should not be null");

    View<T> self = this;
    return new View<>(new AbstractIterator<T>() {

      private boolean dropped_ = false;

      @Override
      protected T computeNext() {
        if (!dropped_) {
          while (self.hasNext()) {
            T e = self.next();
            if (predicate.test(e)) {
              dropped_ = true;
              return e;
            }
          }
          dropped_ = true;
        }
        return self.hasNext() ? self.next() : endOfData();
      }
    });
  }

  /**
   * Returns a view consisting of the results of applying the given function to the elements of this
   * view.
   *
   * @param fn the function to apply.
   * @param <U>
   * @return a new {@link View}.
   */
  public <U> View<U> map(Function<? super T, ? extends U> fn) {
    return of(Iterators.transform(this, fn::apply));
  }

  /**
   * Returns a view consisting of the elements of this view matching the given predicate.
   * 
   * @param predicate the predicate to satisfy.
   * @return a new {@link View}.
   */
  public View<T> filter(Predicate<? super T> predicate) {
    return new View<>(Iterators.filter(this, predicate::test));
  }

  /**
   * Peek each element.
   *
   * @param consumer the action to perform on each element.
   * @return a new {@link View}.
   */
  public View<T> peek(Consumer<T> consumer) {

    Preconditions.checkNotNull(consumer, "consumer should not be null");

    return map(x -> {
      consumer.accept(x);
      return x;
    });
  }

  /**
   * Combines two views into a single view. The returned view iterates across the elements of the
   * current view, followed by the elements of the other view.
   * 
   * @param view the other {@link View}.
   * @return a new {@link View}.
   */
  public View<T> concat(View<? extends T> view) {

    Preconditions.checkNotNull(view, "view should not be null");

    return new View<>(Iterators.concat(this, view));
  }

  /**
   * Divides a view into unmodifiable sublists of the given size (the final list may be smaller).
   *
   * @param size the size of each list.
   * @return a new {@link View}.
   */
  public View<List<T>> partition(int size) {
    return new View<>(Iterators.partition(this, size));
  }

  /**
   * Remove consecutive duplicates from the current view.
   *
   * The view must be sorted.
   *
   * @return a new {@link View}.
   */
  public View<T> dedupSorted() {
    PeekingIterator<T> stream = Iterators.peekingIterator(this);
    return new View<>(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {

        if (!stream.hasNext()) {
          return endOfData();
        }

        @Var
        T curr = stream.next();

        while (stream.hasNext() && curr.equals(stream.peek())) {
          curr = stream.next();
        }
        return curr;
      }
    });
  }

  /**
   * Returns elements that are presents in the first view but not in the second one. Duplicate
   * elements are returned only once.
   *
   * Both views must be sorted.
   *
   * @param view the other {@link View}.
   * @return a new {@link View}.
   */
  public View<T> diffSorted(View<? extends Comparable<T>> view) {

    Preconditions.checkNotNull(view, "view should not be null");

    PeekingIterator<T> thisStream = Iterators.peekingIterator(this);
    PeekingIterator<? extends Comparable<T>> thatStream = Iterators.peekingIterator(view);

    return new View<>(new AbstractIterator<T>() {

      private Comparable<T> cur2_;

      @Override
      protected T computeNext() {

        while (thisStream.hasNext()) {

          T cur1 = thisStream.next();

          while (thatStream.hasNext() && (cur2_ == null || cur2_.compareTo(cur1) < 0)) {
            cur2_ = thatStream.next();
          }

          if (cur2_ != null && cur2_.compareTo(cur1) < 0) {
            return cur1;
          }
          if (cur2_ == null || cur2_.compareTo(cur1) > 0) {
            return cur1;
          }
        }
        return endOfData();
      }
    });
  }

  /**
   * Returns elements that are presents in both views. Duplicate elements are returned only once.
   *
   * Both views must be sorted.
   *
   * @param view the other {@link View}.
   * @return a new {@link View}.
   */
  public View<T> intersectSorted(View<? extends Comparable<T>> view) {

    Preconditions.checkNotNull(view, "view should not be null");

    PeekingIterator<T> thisStream = Iterators.peekingIterator(this);
    PeekingIterator<? extends Comparable<T>> thatStream = Iterators.peekingIterator(view);

    return new View<>(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {

        @Var
        T cur = null;

        while (thisStream.hasNext() && thatStream.hasNext()) {

          cur = thisStream.next();

          while (thatStream.hasNext() && thatStream.peek().compareTo(cur) < 0) {
            thatStream.next();
          }

          if (!thatStream.hasNext()) {
            return endOfData();
          }

          int cmp = thatStream.peek().compareTo(cur);

          if (cmp == 0) {
            while (thisStream.hasNext() && thisStream.peek().equals(cur)) {
              cur = thisStream.next();
            }
            while (thatStream.hasNext() && thatStream.peek().equals(cur)) {
              cur = (T) thatStream.next();
            }
            return cur;
          }
        }
        return endOfData();
      }
    });
  }

  /**
   * Flatten a view. Optionally map the view entries at the same time.
   *
   * @param fn the mapping function.
   * @param <U>
   * @return a new {@link View}.
   */
  public <U> View<U> flatten(Function<T, View<U>> fn) {

    Preconditions.checkNotNull(fn, "fn should not be null");

    PeekingIterator<T> stream = Iterators.peekingIterator(this);

    return new View<>(new AbstractIterator<U>() {

      private View<U> view_;

      @Override
      protected U computeNext() {

        if (view_ == null || !view_.hasNext()) {

          view_ = null;

          while (stream.hasNext() && (view_ == null || !view_.hasNext())) {

            View<U> view = fn.apply(stream.next());

            if (view != null && view.hasNext()) {
              view_ = view;
            }
          }

          if (view_ == null || !view_.hasNext()) {
            return endOfData();
          }
        }
        return view_.next();
      }
    });
  }

  /**
   * Merge the output of one or more sorted views with the output of the current view. The
   * assumption is that all views (including this one) are sorted in non-descending order.
   *
   * @param views the views to merge with the output of the current view.
   * @param comparator the comparator used to merge the output of each view.
   * @return a new {@link View}.
   */
  public View<T> merge(Iterable<? extends View<? extends T>> views,
      Comparator<? super T> comparator) {

    Preconditions.checkNotNull(views, "views should not be null");
    Preconditions.checkNotNull(comparator, "comparator should not be null");

    List<View<? extends T>> list = new ArrayList<>();
    list.add(this);
    views.forEach(list::add);

    return new View<>(Iterators.mergeSorted(list, comparator));
  }

  /**
   * Preload the next {@code capacity} view elements.
   *
   * @param capacity the maximum number of element to preload.
   * @return a new {@link View}.
   */
  public View<T> bufferize(int capacity) {
    return bufferize(capacity, defaultExecutor_);
  }

  /**
   * Preload the next {@code capacity} view elements.
   * 
   * @param capacity the maximum number of element to preload.
   * @param executorService the executor in charge of preloading the next element.
   * @return a new {@link View}.
   */
  public View<T> bufferize(int capacity, ExecutorService executorService) {

    Preconditions.checkNotNull(executorService, "executorService should not be null");

    if (capacity <= 0) {
      return this;
    }

    // Temporary storage for an element we fetched but could not fit in the queue
    AtomicReference<T> overflow = new AtomicReference<>();
    BlockingQueue<T> queue = new ArrayBlockingQueue<>(capacity);
    View<T> self = this;
    Runnable inserter = new Runnable() {

      @SuppressWarnings("unchecked")
      public void run() {

        @Var
        T next = (T) END_MARKER;

        if (self.hasNext()) {

          next = self.next();

          // ArrayBlockingQueue does not allow nulls
          if (next == null) {
            next = (T) NULL_MARKER;
          }
        }
        if (queue.offer(next)) {

          // Keep buffering elements as long as we can
          if (next != END_MARKER) {
            executorService.submit(this);
          }
        } else {

          // Save the element
          // This also signals to the iterator that the inserter thread is blocked
          overflow.lazySet(next);
        }
      }
    };

    // Fetch the first element. The inserter will resubmit itself as necessary to fetch more
    // elements.
    executorService.submit(inserter);

    return new View<>(new AbstractIterator<T>() {

      @Override
      protected T computeNext() {
        try {

          T next = queue.take();
          T overflowElem = overflow.getAndSet(null);

          if (overflowElem != null) {

            // There is now a space in the queue
            queue.put(overflowElem);

            // Awaken the inserter thread
            executorService.submit(inserter);
          }
          if (next == END_MARKER) {
            return endOfData();
          }
          if (next == NULL_MARKER) {
            return null;
          }
          return next;
        } catch (InterruptedException e) {
          // TODO
          Thread.currentThread().interrupt();
          return endOfData();
        }
      }
    });
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
