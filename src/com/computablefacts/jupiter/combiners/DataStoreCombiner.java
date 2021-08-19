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
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class DataStoreCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    Set<String> values = new HashSet<>();
    iter.forEachRemaining(value -> values.addAll(Lists.newArrayList(Splitter
        .on(Constants.SEPARATOR_NUL).trimResults().omitEmptyStrings().split(value.toString()))));

    return new Value(Joiner.on(Constants.SEPARATOR_NUL).join(values));
  }
}
