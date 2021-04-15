package com.computablefacts.jupiter.combiners;

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
import org.apache.datasketches.theta.Sketch;

import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.termstore.MySketch;
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
      return new Value(); // TODO : add trace?
    }

    String cf = key.getColumnFamily().toString();

    if (cf.endsWith("_FIDX") || cf.endsWith("_BIDX")) {
      return reduceIndex(iter);
    }
    if (cf.endsWith("_FCNT") || cf.endsWith("_BCNT")) {
      return reduceCount(iter);
    }
    if (cf.endsWith("_VIZ")) {
      return reduceFieldVisibility(iter);
    }
    if (cf.endsWith("_LU")) {
      return reduceFieldLastUpdate(iter);
    }
    if (cf.endsWith("_DB") || cf.endsWith("_DT")) {
      return reduceSketches(iter);
    }
    return new Value();
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
      sum += Long.parseLong(iter.next().toString(), 10);
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
        sum += Long.parseLong(value, 10);
      } else {
        sum += Long.parseLong(value.substring(0, index), 10);
        builder.append(value.substring(index + 1));
      }
    }

    if (builder.length() <= 0) {
      return new Value(Long.toString(sum, 10));
    }
    return new Value(Long.toString(sum, 10) + Constants.SEPARATOR_NUL + builder.toString());
  }

  private Value reduceSketches(Iterator<Value> iter) {

    List<byte[]> sketches =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false)
            .map(Value::get).collect(Collectors.toList());
    Sketch sketch = MySketch.union(sketches);

    return new Value(sketch.toByteArray());
  }
}
