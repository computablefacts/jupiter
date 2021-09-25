package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;
import static com.computablefacts.jupiter.storage.termstore.TermStore.BACKWARD_COUNT;
import static com.computablefacts.jupiter.storage.termstore.TermStore.BACKWARD_INDEX;
import static com.computablefacts.jupiter.storage.termstore.TermStore.DISTINCT_BUCKETS;
import static com.computablefacts.jupiter.storage.termstore.TermStore.DISTINCT_TERMS;
import static com.computablefacts.jupiter.storage.termstore.TermStore.FORWARD_COUNT;
import static com.computablefacts.jupiter.storage.termstore.TermStore.FORWARD_INDEX;
import static com.computablefacts.jupiter.storage.termstore.TermStore.LAST_UPDATE;
import static com.computablefacts.jupiter.storage.termstore.TermStore.TOP_TERMS;
import static com.computablefacts.jupiter.storage.termstore.TermStore.VISIBILITY;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.theta.Sketch;

import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.termstore.ThetaSketch;
import com.computablefacts.jupiter.storage.termstore.TopKSketch;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
public class TermStoreCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    if (key == null || key.getColumnFamily() == null) {
      return VALUE_EMPTY; // TODO : add trace?
    }

    String cf = key.getColumnFamily().toString();

    if (cf.equals(FORWARD_INDEX) || cf.equals(BACKWARD_INDEX)) {
      return reduceIndex(iter);
    }
    if (cf.equals(FORWARD_COUNT) || cf.equals(BACKWARD_COUNT)) {
      return reduceCount(iter);
    }
    if (cf.equals(VISIBILITY)) {
      return reduceFieldVisibility(iter);
    }
    if (cf.equals(LAST_UPDATE)) {
      return reduceFieldLastUpdate(iter);
    }
    if (cf.equals(DISTINCT_BUCKETS)) {
      return reduceCount(iter);
    }
    if (cf.equals(DISTINCT_TERMS)) {
      return reduceThetaSketches(iter);
    }
    if (cf.equals(TOP_TERMS)) {
      return reduceTopKSketches(iter);
    }
    return VALUE_EMPTY;
  }

  private Value reduceFieldLastUpdate(Iterator<Value> iter) {

    List<String> timestamps = new ArrayList<>();

    while (iter.hasNext()) {
      timestamps.add(iter.next().toString());
    }

    timestamps.sort(String::compareTo);
    return new Value(timestamps.get(timestamps.size() - 1));
  }

  private Value reduceFieldVisibility(Iterator<Value> iter) {

    Set<String> labels = new HashSet<>();

    while (iter.hasNext()) {
      labels.addAll(
          Lists.newArrayList(Splitter.on(Constants.SEPARATOR_NUL).split(iter.next().toString())));
    }
    return new Value(Joiner.on(Constants.SEPARATOR_NUL).join(labels));
  }

  private Value reduceCount(Iterator<Value> iter) {

    @Var
    long sum = 0L;

    while (iter.hasNext()) {
      sum += tryParseLong(iter.next().toString(), 0);
    }
    return new Value(Long.toString(sum, 10));
  }

  private Value reduceIndex(Iterator<Value> iter) {

    @Var
    long sum = 0L;
    StringBuilder builder = new StringBuilder();

    while (iter.hasNext()) {

      if (builder.length() > 0 && builder.charAt(builder.length() - 1) != Constants.SEPARATOR_NUL) {
        builder.append(Constants.SEPARATOR_NUL);
      }

      String value = iter.next().toString();
      int index = value.indexOf(Constants.SEPARATOR_NUL);

      if (index < 0) {
        sum += tryParseLong(value, 0);
      } else {
        sum += tryParseLong(value.substring(0, index), 0);
        builder.append(value.substring(index + 1));
      }
    }

    if (builder.length() <= 0) {
      return new Value(Long.toString(sum, 10));
    }
    return new Value(Long.toString(sum, 10) + Constants.SEPARATOR_NUL + builder);
  }

  private Value reduceThetaSketches(Iterator<Value> iter) {

    List<byte[]> sketches =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false)
            .map(Value::get).collect(Collectors.toList());
    Sketch sketch = ThetaSketch.union(sketches);

    return new Value(sketch.toByteArray());
  }

  private Value reduceTopKSketches(Iterator<Value> iter) {

    List<byte[]> sketches =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false)
            .map(Value::get).collect(Collectors.toList());
    ItemsSketch<String> sketch = TopKSketch.union(sketches);

    return new Value(sketch.toByteArray(new ArrayOfStringsSerDe()));
  }

  private long tryParseLong(String str, long defaultValue) {
    try {
      return Long.parseLong(str, 10);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
