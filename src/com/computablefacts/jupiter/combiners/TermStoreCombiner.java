package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;
import static com.computablefacts.jupiter.storage.termstore.TermStore.*;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import com.computablefacts.jupiter.storage.Constants;
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
    return VALUE_EMPTY;
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

  private long tryParseLong(String str, long defaultValue) {
    try {
      return Long.parseLong(str, 10);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
