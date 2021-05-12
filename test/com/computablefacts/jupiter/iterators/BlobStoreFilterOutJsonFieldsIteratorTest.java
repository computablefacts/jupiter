package com.computablefacts.jupiter.iterators;

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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class BlobStoreFilterOutJsonFieldsIteratorTest {

  @Test
  public void testNullFields() {

    BlobStoreFilterOutJsonFieldsIterator iterator = new BlobStoreFilterOutJsonFieldsIterator();
    IteratorSetting iteratorSetting =
        new IteratorSetting(1, BlobStoreFilterOutJsonFieldsIterator.class);
    BlobStoreFilterOutJsonFieldsIterator.setFieldsToKeep(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNoMatchingFields() throws Exception {

    BlobStoreFilterOutJsonFieldsIterator iterator = iterator(Sets.newHashSet());

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testOneMatchingField() throws Exception {

    BlobStoreFilterOutJsonFieldsIterator iterator = iterator(Sets.newHashSet("name"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"name\":\"John\"}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"name\":\"John\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testTwoMatchingFields() throws Exception {

    BlobStoreFilterOutJsonFieldsIterator iterator = iterator(Sets.newHashSet("age", "city"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"age\":31,\"city\":\"New York\"}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"age\":31,\"city\":\"New York\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  private BlobStoreFilterOutJsonFieldsIterator iterator(Set<String> fields) throws IOException {

    BlobStoreFilterOutJsonFieldsIterator iterator = new BlobStoreFilterOutJsonFieldsIterator();
    IteratorSetting setting = new IteratorSetting(1, BlobStoreFilterOutJsonFieldsIterator.class);
    BlobStoreFilterOutJsonFieldsIterator.setFieldsToKeep(setting, fields);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("KEY_1", "DATASET_1", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));
    map.put(new Key("KEY_2", "DATASET_1", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));
    map.put(new Key("KEY_3", "DATASET_1", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));

    map.put(new Key("KEY_1", "DATASET_2", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));
    map.put(new Key("KEY_2", "DATASET_2", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));
    map.put(new Key("KEY_3", "DATASET_2", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));

    return map;
  }

  private String json() {
    return "{\"name\":\"John\", \"age\":31, \"city\":\"New York\"}";
  }
}
