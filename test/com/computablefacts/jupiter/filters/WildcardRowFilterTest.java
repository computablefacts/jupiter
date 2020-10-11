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

public class WildcardRowFilterTest {

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

      String row = iterator.getTopKey().getRow().toString();

      Assert.assertTrue(row.startsWith("OBJECT_1\0PREDICATE_"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(12, nbMatch);
  }

  @Test
  public void testMatchSuffix() throws IOException {

    WildcardFilter iterator = iterator("*\0PREDICATE_1");

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String row = iterator.getTopKey().getRow().toString();

      Assert.assertEquals("OBJECT_1\0PREDICATE_1", row);

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsOr() throws IOException {

    WildcardFilter iterator =
        iterator(Lists.newArrayList("*\0PREDICATE_1", "*\0PREDICATE_2"), false);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String row = iterator.getTopKey().getRow().toString();

      Assert.assertTrue(row.equals("OBJECT_1\0PREDICATE_1") || row.equals("OBJECT_1\0PREDICATE_2"));

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(6, nbMatch);
  }

  @Test
  public void testMatchMultiplePatternsAnd() throws IOException {

    WildcardFilter iterator = iterator(Lists.newArrayList("*\0PREDICATE_1", "OBJECT_1\0*"), true);

    @Var
    int nbMatch = 0;

    while (iterator.hasTop()) {

      String row = iterator.getTopKey().getRow().toString();

      Assert.assertEquals("OBJECT_1\0PREDICATE_1", row);

      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  private WildcardFilter iterator(String pattern) throws IOException {
    return iterator(Lists.newArrayList(pattern), false);
  }

  private WildcardFilter iterator(List<String> patterns, boolean and) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnRow(setting);

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

    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_1", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_2", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_1", "DATASET_3", "SUBJECT_1", 0), new Value("1"));

    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_1", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_2", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_2", "DATASET_3", "SUBJECT_1", 0), new Value("1"));

    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_1", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_2", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_3", "DATASET_3", "SUBJECT_1", 0), new Value("1"));

    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_1", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_2", "SUBJECT_1", 0), new Value("1"));
    map.put(new Key("OBJECT_1\0PREDICATE_4", "DATASET_3", "SUBJECT_1", 0), new Value("1"));

    return map;
  }
}
