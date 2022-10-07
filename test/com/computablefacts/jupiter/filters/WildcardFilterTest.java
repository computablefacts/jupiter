package com.computablefacts.jupiter.filters;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class WildcardFilterTest {

  @Test
  public void testNullRow() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnRow(iteratorSetting);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullCf() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnColumnFamily(iteratorSetting);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullCq() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnColumnQualifier(iteratorSetting);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullValue() {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnValue(iteratorSetting);
    WildcardFilter.addWildcard(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchNoRow() throws Exception {
    Assert.assertFalse(iteratorRow("").hasTop());
  }

  @Test
  public void testMatchNoCf() throws Exception {
    Assert.assertFalse(iteratorCf("").hasTop());
  }

  @Test
  public void testMatchNoCq() throws Exception {
    Assert.assertFalse(iteratorCq("").hasTop());
  }

  @Test
  public void testMatchNoValue() throws Exception {
    Assert.assertFalse(iteratorValue("").hasTop());
  }

  @Test
  public void testMatchRow() throws Exception {

    WildcardFilter iterator = iteratorRow("????_1");

    @Var int nbMatch = 0;

    while (iterator.hasTop()) {
      String cf = iterator.getTopKey().getColumnFamily().toString();
      Assert.assertEquals("DATASET_1", cf);
      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  @Test
  public void testMatchCf() throws Exception {

    WildcardFilter iterator = iteratorCf("*_1");

    @Var int nbMatch = 0;

    while (iterator.hasTop()) {
      String row = iterator.getTopKey().getRow().toString();
      Assert.assertEquals("TERM_1", row);
      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  @Test
  public void testMatchCq() throws Exception {

    WildcardFilter iterator = iteratorCq("*_1\0FIELD_?");

    @Var int nbMatch = 0;

    while (iterator.hasTop()) {
      String row = iterator.getTopKey().getRow().toString();
      String cf = iterator.getTopKey().getColumnFamily().toString();
      Assert.assertEquals("TERM_1", row);
      Assert.assertEquals("DATASET_1", cf);
      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  @Test
  public void testMatchValue() throws Exception {

    WildcardFilter iterator = iteratorValue("1");

    @Var int nbMatch = 0;

    while (iterator.hasTop()) {
      String row = iterator.getTopKey().getRow().toString();
      String cf = iterator.getTopKey().getColumnFamily().toString();
      Assert.assertEquals("TERM_1", row);
      Assert.assertEquals("DATASET_1", cf);
      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(3, nbMatch);
  }

  @Test
  public void testAnd() throws Exception {

    WildcardFilter iterator = iteratorCq(Sets.newHashSet("DOCID_1*", "*FIELD_1"), false);

    @Var int nbMatch = 0;

    while (iterator.hasTop()) {
      String row = iterator.getTopKey().getRow().toString();
      String cf = iterator.getTopKey().getColumnFamily().toString();
      Assert.assertEquals("TERM_1", row);
      Assert.assertEquals("DATASET_1", cf);
      nbMatch++;
      iterator.next();
    }

    Assert.assertEquals(1, nbMatch);
  }

  @Test
  public void testOr() throws Exception {

    WildcardFilter iterator = iteratorCq(Sets.newHashSet("DOCID_1*", "*FIELD_1"), true);

    @Var int nbMatchDocId1 = 0;
    @Var int nbMatchField1 = 0;

    while (iterator.hasTop()) {

      String cq = iterator.getTopKey().getColumnQualifier().toString();

      if (cq.startsWith("DOCID_1")) {
        nbMatchDocId1++;
      }
      if (cq.endsWith("FIELD_1")) {
        nbMatchField1++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, nbMatchDocId1);
    Assert.assertEquals(2, nbMatchField1);
  }

  private WildcardFilter iteratorRow(String pattern) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnRow(setting);
    WildcardFilter.addWildcard(setting, pattern);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private WildcardFilter iteratorCf(String pattern) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnColumnFamily(setting);
    WildcardFilter.addWildcard(setting, pattern);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private WildcardFilter iteratorCq(String pattern) throws IOException {
    return iteratorCq(Sets.newHashSet(pattern), false);
  }

  private WildcardFilter iteratorCq(Set<String> patterns, boolean isOr) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnColumnQualifier(setting);

    for (String pattern : patterns) {
      WildcardFilter.addWildcard(setting, pattern);
    }

    if (isOr) {
      WildcardFilter.setOr(setting);
    } else {
      WildcardFilter.setAnd(setting);
    }

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private WildcardFilter iteratorValue(String pattern) throws IOException {

    WildcardFilter iterator = new WildcardFilter();
    IteratorSetting setting = new IteratorSetting(1, WildcardFilter.class);
    WildcardFilter.applyOnValue(setting);
    WildcardFilter.addWildcard(setting, pattern);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_1", 0), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_2", 0), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_3", 0), new Value("1"));

    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_1", 0), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_2", 0), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_3", 0), new Value("2"));

    return map;
  }
}
