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

public class WildcardValueFilterTest {

  @Test
  public void testNullPattern() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchExact() throws IOException {

    WildcardFilter iterator = iterator("b\0b\0b");

    Assert.assertFalse(iterator.hasTop());
  }

  @Test
  public void testMatchPrefix() throws IOException {

    WildcardFilter iterator = iterator("a\0a\0?");

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String value = iterator.getTopValue().toString();

      Assert.assertTrue(value.startsWith("a\0a\0"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(1, nbMatch);
  }

  @Test
  public void testMatchSuffix() throws IOException {

    WildcardFilter iterator = iterator("*\0a\0a");

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String value = iterator.getTopValue().toString();

      Assert.assertTrue(value.endsWith("\0a\0a"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(4, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsOr() throws IOException {

    WildcardFilter iterator = iterator(Lists.newArrayList("a\0*", "*\0b\0a"), false);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String value = iterator.getTopValue().toString();

      Assert.assertTrue(value.startsWith("a\0") || value.endsWith("\0b\0a"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(6, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsAnd() throws IOException {

    WildcardFilter iterator = iterator(Lists.newArrayList("a\0*", "*\0b\0a"), true);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String value = iterator.getTopValue().toString();

      Assert.assertEquals("a\0b\0a", value);

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(1, nbMatch);
  }

  private WildcardFilter iterator(String pattern) throws IOException {
    return iterator(Lists.newArrayList(pattern), false);
  }

  private WildcardFilter iterator(List<String> patterns, boolean and) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnValue(setting);

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

    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_1", "SUBJECT_1", 0), new Value("a\0a\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_2", "SUBJECT_1", 0), new Value("a\0b\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_3", "SUBJECT_1", 0), new Value("a\0c\0a"));

    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_1", "SUBJECT_1", 0), new Value("b\0a\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_2", "SUBJECT_1", 0), new Value("b\0b\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_3", "SUBJECT_1", 0), new Value("b\0c\0a"));

    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_1", "SUBJECT_1", 0), new Value("c\0a\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_2", "SUBJECT_1", 0), new Value("c\0b\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_3", "SUBJECT_1", 0), new Value("c\0c\0a"));

    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_1", "SUBJECT_1", 0), new Value("d\0a\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_2", "SUBJECT_1", 0), new Value("d\0b\0a"));
    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_3", "SUBJECT_1", 0), new Value("d\0c\0a"));

    return map;
  }
}
