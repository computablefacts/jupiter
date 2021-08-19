package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.TEXT_HASH_INDEX;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.hadoop.io.Text;

import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;

@Deprecated
@CheckReturnValue
public class DataStoreHashIndexCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    if (key == null || key.getColumnFamily() == null) {
      return VALUE_EMPTY; // TODO : add trace?
    }

    Text cf = key.getColumnFamily();

    if (cf.equals(TEXT_HASH_INDEX)) {
      return reduceHashIndex(iter);
    }
    return VALUE_EMPTY;
  }

  private Value reduceHashIndex(Iterator<Value> iter) {

    Set<String> docsIds = new HashSet<>();
    iter.forEachRemaining(value -> docsIds.addAll(Lists.newArrayList(Splitter
        .on(Constants.SEPARATOR_NUL).trimResults().omitEmptyStrings().split(value.toString()))));

    return new Value(Joiner.on(Constants.SEPARATOR_NUL).join(docsIds));
  }
}
