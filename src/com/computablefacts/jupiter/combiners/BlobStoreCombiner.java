package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_ARRAY;

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
public class BlobStoreCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    if (key == null || key.getColumnFamily() == null) {
      return VALUE_EMPTY; // TODO : add trace?
    }

    String cf = key.getColumnFamily().toString();

    if (cf.startsWith(TYPE_ARRAY)) {

      Set<String> values = new HashSet<>();
      iter.forEachRemaining(value -> values.addAll(Lists.newArrayList(Splitter
          .on(Constants.SEPARATOR_NUL).trimResults().omitEmptyStrings().split(value.toString()))));

      return new Value(Joiner.on(Constants.SEPARATOR_NUL).join(values));
    }
    return VALUE_EMPTY;
  }
}
