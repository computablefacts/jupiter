package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.BloomFilters;
import com.google.errorprone.annotations.Var;

public class BloomFilterFilterTest {

  @Test
  public void testNullRow() {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnRow(iteratorSetting);
    BloomFilterFilter.setBloomFilter(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullCf() {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnFamily(iteratorSetting);
    BloomFilterFilter.setBloomFilter(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullCq() {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnQualifier(iteratorSetting);
    BloomFilterFilter.setBloomFilter(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullValue() {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnValue(iteratorSetting);
    BloomFilterFilter.setBloomFilter(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchNoRow() throws Exception {
    Assert.assertFalse(iteratorRow(new BloomFilters<>()).hasTop());
  }

  @Test
  public void testMatchNoCf() throws Exception {
    Assert.assertFalse(iteratorCf(new BloomFilters<>()).hasTop());
  }

  @Test
  public void testMatchNoCq() throws Exception {
    Assert.assertFalse(iteratorCq(new BloomFilters<>()).hasTop());
  }

  @Test
  public void testMatchNoValue() throws Exception {
    Assert.assertFalse(iteratorValue(new BloomFilters<>()).hasTop());
  }

  @Test
  public void testMatchRow() throws Exception {

    BloomFilters<String> bf = new BloomFilters<>();
    bf.put("TERM_1");

    BloomFilterFilter iterator = iteratorRow(bf);

    @Var
    int nbMatch = 0;

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

    BloomFilters<String> bf = new BloomFilters<>();
    bf.put("DATASET_1");

    BloomFilterFilter iterator = iteratorCf(bf);

    @Var
    int nbMatch = 0;

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

    BloomFilters<String> bf = new BloomFilters<>();
    bf.put("DOCID_1\0FIELD_1");

    BloomFilterFilter iterator = iteratorCq(bf);

    @Var
    int nbMatch = 0;

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
  public void testMatchValue() throws Exception {

    BloomFilters<String> bf = new BloomFilters<>();
    bf.put("1");

    BloomFilterFilter iterator = iteratorValue(bf);

    @Var
    int nbMatch = 0;

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

  private BloomFilterFilter iteratorRow(BloomFilters<String> bf) throws IOException {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting setting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnRow(setting);
    BloomFilterFilter.setBloomFilter(setting, bf);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private BloomFilterFilter iteratorCf(BloomFilters<String> bf) throws IOException {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting setting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnFamily(setting);
    BloomFilterFilter.setBloomFilter(setting, bf);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private BloomFilterFilter iteratorCq(BloomFilters<String> bf) throws IOException {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting setting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnQualifier(setting);
    BloomFilterFilter.setBloomFilter(setting, bf);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private BloomFilterFilter iteratorValue(BloomFilters<String> bf) throws IOException {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting setting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnValue(setting);
    BloomFilterFilter.setBloomFilter(setting, bf);

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
