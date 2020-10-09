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
package com.computablefacts.jupiter.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;

public class FlattenIteratorTest {

  @Test
  public void testFlattenCollection() {

    List<String> listComputed = Lists
        .newArrayList(new FlattenIterator<>(list(), entry -> entry.getSecond().stream().map(e -> {

          String row = e.getKey().getRow().toString();
          String cf = e.getKey().getColumnFamily().toString();
          String cq = e.getKey().getColumnQualifier().toString();
          String val = e.getValue().toString();

          return row + Constants.SEPARATOR_NUL + cq + Constants.SEPARATOR_NUL + val;
        }).collect(Collectors.toList())));

    List<String> listExpected = Lists.newArrayList(
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1");

    Assert.assertEquals(18, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testFlattenAlreadyFlatCollection() {

    List<String> listComputed =
        Lists.newArrayList(new FlattenIterator<>(map().entrySet().iterator(), entry -> {

          String row = entry.getKey().getRow().toString();
          String cf = entry.getKey().getColumnFamily().toString();
          String cq = entry.getKey().getColumnQualifier().toString();
          String val = entry.getValue().toString();

          return Lists
              .newArrayList(row + Constants.SEPARATOR_NUL + cq + Constants.SEPARATOR_NUL + val);
        }));

    List<String> listExpected = Lists.newArrayList(
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_1" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_1" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_2" + Constants.SEPARATOR_NUL
            + "1",
        "SUBJECT_2" + Constants.SEPARATOR_NUL + "OBJECT_1\0PREDICATE_3" + Constants.SEPARATOR_NUL
            + "1");

    Assert.assertEquals(18, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  private Iterator<Pair<String, List<Map.Entry<Key, Value>>>> list() {
    return new GroupByRowIterator(map().entrySet().iterator());
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_1", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_1", "DATASET_3", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_3", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_3", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_2", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_2", "DATASET_3", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_3", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_3", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    return map;
  }

  final static class GroupByRowIterator
      extends AbstractIterator<Pair<String, List<Map.Entry<Key, Value>>>> {

    private final PeekingIterator<Map.Entry<Key, Value>> iterator_;

    public GroupByRowIterator(Iterator<Map.Entry<Key, Value>> iterator) {
      iterator_ = new PeekingIterator<>(
          Preconditions.checkNotNull(iterator, "iterator should not be null"));
    }

    @Override
    protected Pair<String, List<Map.Entry<Key, Value>>> computeNext() {

      if (!iterator_.hasNext()) {
        return endOfData();
      }

      @Var
      Text row = null;
      List<Map.Entry<Key, Value>> list = new ArrayList<>();

      while (iterator_.hasNext()
          && (row == null || row.equals(iterator_.peek().getKey().getRow()))) {
        Map.Entry<Key, Value> pair = iterator_.next();
        row = pair.getKey().getRow();
        list.add(pair);
      }
      return new Pair<>(row.toString(), list);
    }
  }
}
