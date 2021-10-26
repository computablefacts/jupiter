package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
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

public class BlobStoreMaskedJsonFieldFilterTest {

  @Test
  public void testNullFieldKey() {

    BlobStoreMaskedJsonFieldFilter iterator = new BlobStoreMaskedJsonFieldFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BlobStoreMaskedJsonFieldFilter.class);
    BlobStoreMaskedJsonFieldFilter.addFilter(iteratorSetting, null, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullFieldValue() {

    BlobStoreMaskedJsonFieldFilter iterator = new BlobStoreMaskedJsonFieldFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BlobStoreMaskedJsonFieldFilter.class);
    BlobStoreMaskedJsonFieldFilter.addFilter(iteratorSetting, "name", null);

    Assert.assertTrue(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testOneFilter() throws Exception {

    BlobStoreMaskedJsonFieldFilter iterator = iterator("name", "Tom Cruise", null, null);

    Set<String> jsons = new HashSet<>();

    while (iterator.hasTop()) {
      jsons.add(iterator.getTopValue().toString());
      iterator.next();
    }

    Assert.assertEquals(1, jsons.size());
    Assert.assertTrue(jsons.contains("{\"name\":\"Tom Cruise\",\"age\":56}"));
  }

  @Test
  public void testMoreThanOneFilter() throws Exception {

    BlobStoreMaskedJsonFieldFilter iterator = iterator("age", "73", "name", "Robert Downey Jr.");

    Set<String> jsons = new HashSet<>();

    while (iterator.hasTop()) {
      jsons.add(iterator.getTopValue().toString());
      iterator.next();
    }

    Assert.assertEquals(1, jsons.size());
    Assert.assertTrue(jsons.contains("{\"name\":\"Robert Downey Jr.\",\"age\":73}"));
  }

  private BlobStoreMaskedJsonFieldFilter iterator(String key1, String value1, String key2,
      String value2) throws IOException {

    BlobStoreMaskedJsonFieldFilter iterator = new BlobStoreMaskedJsonFieldFilter();
    IteratorSetting setting = new IteratorSetting(1, BlobStoreMaskedJsonFieldFilter.class);

    if (key1 != null) {
      BlobStoreMaskedJsonFieldFilter.addFilter(setting, key1, value1);
    }
    if (key2 != null) {
      BlobStoreMaskedJsonFieldFilter.addFilter(setting, key2, value2);
    }

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("PERSONS\0T1", "JSO", "", 0),
        new Value("{\"name\":\"Tom Cruise\",\"age\":56}"));
    map.put(new Key("PERSONS\0T2", "JSO", "", 0),
        new Value("{\"name\":\"Robert Downey Jr.\",\"age\":73}"));
    map.put(new Key("PERSONS\0T3", "JSO", "", 0), new Value("{}"));

    return map;
  }
}
