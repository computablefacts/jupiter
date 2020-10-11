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

public class BloomFilterCqFilterTest {

  @Test
  public void testNullPattern() {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnQualifier(iteratorSetting);
    BloomFilterFilter.setBloomFilter(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchNone() throws IOException {

    BloomFilterFilter iterator = iterator(new BloomFilters<>());

    Assert.assertFalse(iterator.hasTop());
  }

  @Test
  public void testMatchMoreThanOne() throws IOException {

    BloomFilters<String> bf = new BloomFilters<>();
    bf.put("OBJECT_1\0PREDICATE_1");

    BloomFilterFilter iterator = iterator(bf);

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

  private BloomFilterFilter iterator(BloomFilters<String> bf) throws IOException {

    BloomFilterFilter iterator = new BloomFilterFilter();
    IteratorSetting setting = new IteratorSetting(1, BloomFilterFilter.class);
    BloomFilterFilter.applyOnColumnQualifier(setting);
    BloomFilterFilter.setBloomFilter(setting, bf);

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
