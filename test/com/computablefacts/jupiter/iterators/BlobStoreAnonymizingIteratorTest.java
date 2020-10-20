package com.computablefacts.jupiter.iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.storage.Constants;
import com.google.errorprone.annotations.Var;

public class BlobStoreAnonymizingIteratorTest {

  @Test
  public void testNull() {

    BlobStoreAnonymizingIterator iterator = new BlobStoreAnonymizingIterator();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BlobStoreAnonymizingIterator.class);
    BlobStoreAnonymizingIterator.setAuthorizations(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testExactValue() throws Exception {

    BlobStoreAnonymizingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DS1", "DS2"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("1", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("2", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testMaskedValue() throws Exception {

    BlobStoreAnonymizingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DS1"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("1", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals(Constants.VALUE_ANONYMIZED.toString(), value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  private BlobStoreAnonymizingIterator iterator(Authorizations auths) throws IOException {

    BlobStoreAnonymizingIterator iterator = new BlobStoreAnonymizingIterator();
    IteratorSetting setting = new IteratorSetting(1, BlobStoreAnonymizingIterator.class);
    BlobStoreAnonymizingIterator.setAuthorizations(setting, auths);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("KEY_1", "DATASET_1", "", new ColumnVisibility("ADM|DS1"), 0), new Value("1"));
    map.put(new Key("KEY_2", "DATASET_1", "", new ColumnVisibility("ADM|DS1"), 0), new Value("1"));
    map.put(new Key("KEY_3", "DATASET_1", "", new ColumnVisibility("ADM|DS1"), 0), new Value("1"));

    map.put(new Key("KEY_1", "DATASET_2", "", new ColumnVisibility("ADM|DS2"), 0), new Value("2"));
    map.put(new Key("KEY_2", "DATASET_2", "", new ColumnVisibility("ADM|DS2"), 0), new Value("2"));
    map.put(new Key("KEY_3", "DATASET_2", "", new ColumnVisibility("ADM|DS2"), 0), new Value("2"));

    return map;
  }
}
