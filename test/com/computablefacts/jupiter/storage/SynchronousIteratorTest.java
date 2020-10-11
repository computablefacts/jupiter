package com.computablefacts.jupiter.storage;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SynchronousIteratorTest {

  @Test(expected = NullPointerException.class)
  public void testFirstIteratorIsNull() {
    Iterator<Key> iterator = new SynchronousIterator<>(null, map().iterator());
  }

  @Test(expected = NullPointerException.class)
  public void testSecondIteratorIsNull() {
    Iterator<Key> iterator = new SynchronousIterator<>(map().iterator(), null);
  }

  @Test(expected = NullPointerException.class)
  public void testBothIteratorsAreNull() {
    Iterator<Key> iterator = new SynchronousIterator<>(null, null);
  }

  @Test
  public void testFirstIteratorIsEmpty() {

    List<Key> listComputed = Lists.newArrayList(
        new SynchronousIterator<>(Lists.<Key>newArrayList().iterator(), map().iterator()));

    List<Key> listExpected = Lists.newArrayList();

    Assert.assertEquals(0, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testSecondIteratorIsEmpty() {

    List<Key> listComputed = Lists.newArrayList(
        new SynchronousIterator<>(map().iterator(), Lists.<Key>newArrayList().iterator()));

    List<Key> listExpected = Lists.newArrayList();

    Assert.assertEquals(0, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testBothIteratorsAreEmpty() {

    List<Key> listComputed =
        Lists.newArrayList(new SynchronousIterator<>(Lists.<Key>newArrayList().iterator(),
            Lists.<Key>newArrayList().iterator()));

    List<Key> listExpected = Lists.newArrayList();

    Assert.assertEquals(0, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testBothIteratorsAreEquals() {

    List<Key> listComputed =
        Lists.newArrayList(new SynchronousIterator<>(map().iterator(), map().iterator()));

    List<Key> listExpected = Lists.newArrayList(map().iterator());

    Assert.assertEquals(15, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testOddsOnly() {

    List<Key> listComputedForward =
        Lists.newArrayList(new SynchronousIterator<>(map().iterator(), mapOdd().iterator()));

    List<Key> listComputedBackward =
        Lists.newArrayList(new SynchronousIterator<>(mapOdd().iterator(), map().iterator()));

    List<Key> listExpected = Lists.newArrayList(mapOdd().iterator());

    Assert.assertEquals(9, listComputedForward.size());
    Assert.assertEquals(listExpected, listComputedForward);

    Assert.assertEquals(9, listComputedBackward.size());
    Assert.assertEquals(listExpected, listComputedBackward);
  }

  @Test
  public void testEvensOnly() {

    List<Key> listComputedForward =
        Lists.newArrayList(new SynchronousIterator<>(map().iterator(), mapEven().iterator()));

    List<Key> listComputedBackward =
        Lists.newArrayList(new SynchronousIterator<>(mapEven().iterator(), map().iterator()));

    List<Key> listExpected = Lists.newArrayList(
        mapEven().stream().filter(k -> !k.getRow().equals(new Text("SUBJECT_6"))).iterator());

    Assert.assertEquals(6, listComputedForward.size());
    Assert.assertEquals(listExpected, listComputedForward);

    Assert.assertEquals(6, listComputedBackward.size());
    Assert.assertEquals(listExpected, listComputedBackward);
  }

  private Set<Key> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    return map.keySet();
  }

  private Set<Key> mapOdd() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_1", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_3", "DATASET_3", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_5", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    return map.keySet();
  }

  private Set<Key> mapEven() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_2", "DATASET_2", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_4", "DATASET_1", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    map.put(new Key("SUBJECT_6", "DATASET_3", "OBJECT_1\0PREDICATE_1", 0), new Value("1"));
    map.put(new Key("SUBJECT_6", "DATASET_3", "OBJECT_1\0PREDICATE_2", 0), new Value("1"));
    map.put(new Key("SUBJECT_6", "DATASET_3", "OBJECT_1\0PREDICATE_3", 0), new Value("1"));

    return map.keySet();
  }
}
