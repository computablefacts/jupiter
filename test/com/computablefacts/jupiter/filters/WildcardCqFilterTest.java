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
package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;

public class WildcardCqFilterTest {

  @Test
  public void testNullPattern() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchExact() throws IOException {

    WildcardFilter iterator = iterator("PREDICATE_1");

    Assert.assertFalse(iterator.hasTop());
  }

  @Test
  public void testMatchPrefix() throws IOException {

    WildcardFilter iterator = iterator("OBJECT_1\0PREDICATE_?");

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String cq = iterator.getTopKey().getColumnQualifier().toString();

      Assert.assertTrue(cq.startsWith("OBJECT_1\0PREDICATE_"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(18, nbMatch);
  }

  @Test
  public void testMatchSuffix() throws IOException {

    WildcardFilter iterator = iterator("*\0PREDICATE_1");

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String cq = iterator.getTopKey().getColumnQualifier().toString();

      Assert.assertEquals("OBJECT_1\0PREDICATE_1", cq);

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(6, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsOr() throws IOException {

    WildcardFilter iterator =
        iterator(Lists.newArrayList("*\0PREDICATE_1", "*\0PREDICATE_2"), false);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String cq = iterator.getTopKey().getColumnQualifier().toString();

      Assert.assertTrue(cq.equals("OBJECT_1\0PREDICATE_1") || cq.equals("OBJECT_1\0PREDICATE_2"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(12, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsAnd() throws IOException {

    WildcardFilter iterator = iterator(Lists.newArrayList("*\0PREDICATE_1", "OBJECT_1\0*"), true);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String cq = iterator.getTopKey().getColumnQualifier().toString();

      Assert.assertEquals("OBJECT_1\0PREDICATE_1", cq);

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(6, nbMatch);
  }

  private WildcardFilter iterator(String pattern) throws IOException {
    return iterator(Lists.newArrayList(pattern), false);
  }

  private WildcardFilter iterator(List<String> patterns, boolean and) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnColumnQualifier(setting);

    if (and) {
      WildcardFilter.setAnd(setting);
    } else {
      WildcardFilter.setOr(setting);
    }

    for (String pattern : patterns) {
      WildcardFilter.addWildcard(setting, pattern);
    }

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
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
}
