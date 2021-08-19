package com.computablefacts.jupiter.filters;

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

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class TermStoreBucketFieldFilterTest {

  @Test
  public void testNullDocs() {

    TermStoreBucketFieldFilter iterator = new TermStoreBucketFieldFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, TermStoreBucketFieldFilter.class);
    TermStoreBucketFieldFilter.setDocsToKeep(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNullFields() {

    TermStoreBucketFieldFilter iterator = new TermStoreBucketFieldFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, TermStoreBucketFieldFilter.class);
    TermStoreBucketFieldFilter.setFieldsToKeep(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testMatchNoDoc() throws Exception {
    Assert.assertFalse(iterator(new BloomFilters<>(), null).hasTop());
  }

  @Test
  public void testMatchNoField() throws Exception {
    Assert.assertFalse(iterator(null, Sets.newHashSet()).hasTop());
  }

  @Test
  public void filterOnDocs() throws Exception {

    BloomFilters<String> bfs = new BloomFilters<>();
    bfs.put("DOCID_1");

    TermStoreBucketFieldFilter iterator = iterator(bfs, null);

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

  @Test
  public void filterOnFields() throws Exception {

    TermStoreBucketFieldFilter iterator = iterator(null, Sets.newHashSet("FIELD_1", "FIELD_2"));

    List<Key> list = new ArrayList<>();

    while (iterator.hasTop()) {
      list.add(iterator.getTopKey());
      iterator.next();
    }

    Assert.assertEquals(1,
        list.stream().filter(e -> e.getColumnFamily().toString().equals("DATASET_1")
            && e.getColumnQualifier().toString().endsWith("FIELD_1")).count());

    Assert.assertEquals(1,
        list.stream()
            .filter(e -> e.getColumnFamily().toString().equals("DATASET_2")
                && e.getColumnQualifier().toString().endsWith("FIELD_1\0" + Term.TYPE_NUMBER))
            .count());

    Assert.assertEquals(1,
        list.stream().filter(e -> e.getColumnFamily().toString().equals("DATASET_1")
            && e.getColumnQualifier().toString().endsWith("FIELD_2")).count());

    Assert.assertEquals(1,
        list.stream()
            .filter(e -> e.getColumnFamily().toString().equals("DATASET_2")
                && e.getColumnQualifier().toString().endsWith("FIELD_2\0" + Term.TYPE_NUMBER))
            .count());
  }

  @Test
  public void filterOnDocsAndFields() throws Exception {

    BloomFilters<String> bfs = new BloomFilters<>();
    bfs.put("DOCID_1");

    TermStoreBucketFieldFilter iterator = iterator(bfs, Sets.newHashSet("FIELD_1", "FIELD_2"));

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

    Assert.assertEquals(2, nbMatch);
  }

  private TermStoreBucketFieldFilter iterator(BloomFilters<String> docsToKeep,
      Set<String> fieldsToKeep) throws IOException {

    TermStoreBucketFieldFilter iterator = new TermStoreBucketFieldFilter();
    IteratorSetting setting = new IteratorSetting(1, TermStoreBucketFieldFilter.class);
    TermStoreBucketFieldFilter.setDocsToKeep(setting, docsToKeep);
    TermStoreBucketFieldFilter.setFieldsToKeep(setting, fieldsToKeep);

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

    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_1\0" + Term.TYPE_NUMBER, 0),
        new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_2\0" + Term.TYPE_NUMBER, 0),
        new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_3\0" + Term.TYPE_NUMBER, 0),
        new Value("2"));

    return map;
  }
}
