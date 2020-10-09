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
package com.computablefacts.jupiter.combiners;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;

public class TermStoreCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    if (key == null || key.getColumnFamily() == null) {
      return new Value(); // TODO : add trace?
    }

    String cf = key.getColumnFamily().toString();

    if (cf.endsWith("IDX")) {
      return reduceIndex(iter);
    }
    if (cf.endsWith("CARD")) {
      return reduceCardinality(iter);
    }
    if (cf.endsWith("CNT")) {
      return reduceCount(iter);
    }
    if (cf.endsWith("VIZ")) {
      return reduceFieldVisibility(iter);
    }
    return new Value();
  }

  private Value reduceFieldVisibility(Iterator<Value> iter) {

    Set<String> labels = new HashSet<>();

    while (iter.hasNext()) {
      labels.addAll(
          Lists.newArrayList(Splitter.on(Constants.SEPARATOR_NUL).split(iter.next().toString())));
    }
    return new Value(Joiner.on(Constants.SEPARATOR_NUL).join(labels));
  }

  private Value reduceCardinality(Iterator<Value> iter) {
    return reduceCount(iter);
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
}
