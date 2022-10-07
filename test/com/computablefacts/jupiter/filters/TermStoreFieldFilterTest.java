package com.computablefacts.jupiter.filters;

import com.computablefacts.jupiter.storage.termstore.Term;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

public class TermStoreFieldFilterTest {

  @Test
  public void testNullFields() {

    TermStoreFieldFilter iterator = new TermStoreFieldFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, TermStoreFieldFilter.class);
    TermStoreFieldFilter.setFieldsToKeep(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchNoField() throws Exception {
    Assert.assertFalse(iterator(Sets.newHashSet()).hasTop());
  }

  @Test
  public void filterOnFields() throws Exception {

    TermStoreFieldFilter iterator = iterator(Sets.newHashSet("FIELD_1", "FIELD_2"));

    List<Key> list = new ArrayList<>();

    while (iterator.hasTop()) {
      list.add(iterator.getTopKey());
      iterator.next();
    }

    Assert.assertEquals(1, list.stream().filter(
        e -> e.getColumnFamily().toString().equals("DATASET_1") && e.getColumnQualifier().toString()
            .endsWith("FIELD_1")).count());

    Assert.assertEquals(1, list.stream().filter(
        e -> e.getColumnFamily().toString().equals("DATASET_2") && e.getColumnQualifier().toString()
            .endsWith("FIELD_1\0" + Term.TYPE_NUMBER)).count());

    Assert.assertEquals(1, list.stream().filter(
        e -> e.getColumnFamily().toString().equals("DATASET_1") && e.getColumnQualifier().toString()
            .endsWith("FIELD_2")).count());

    Assert.assertEquals(1, list.stream().filter(
        e -> e.getColumnFamily().toString().equals("DATASET_2") && e.getColumnQualifier().toString()
            .endsWith("FIELD_2\0" + Term.TYPE_NUMBER)).count());
  }

  private TermStoreFieldFilter iterator(Set<String> fieldsToKeep) throws IOException {

    TermStoreFieldFilter iterator = new TermStoreFieldFilter();
    IteratorSetting setting = new IteratorSetting(1, TermStoreFieldFilter.class);
    TermStoreFieldFilter.setFieldsToKeep(setting, fieldsToKeep);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("TERM_1", "DATASET_1", "FIELD_1", 0), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "FIELD_2", 0), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "FIELD_3", 0), new Value("1"));

    map.put(new Key("TERM_2", "DATASET_2", "FIELD_1\0" + Term.TYPE_NUMBER, 0), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "FIELD_2\0" + Term.TYPE_NUMBER, 0), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "FIELD_3\0" + Term.TYPE_NUMBER, 0), new Value("2"));

    return map;
  }
}
