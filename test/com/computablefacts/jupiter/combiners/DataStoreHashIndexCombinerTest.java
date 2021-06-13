package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.TEXT_HASH_INDEX;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.collect.Lists;

public class DataStoreHashIndexCombinerTest {

  @Test
  public void testReduceNotTheRightColumnFamily() {

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("3"),
        new Value("4"), new Value("5")).iterator();

    DataStoreHashIndexCombiner combiner = new DataStoreHashIndexCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), new Text("dataset")), values);

    Assert.assertEquals("", value.toString());
  }

  @Test
  public void testReduceIteratorHavingNoDuplicates() {

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("3"),
        new Value("4"), new Value("5")).iterator();

    DataStoreHashIndexCombiner combiner = new DataStoreHashIndexCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), TEXT_HASH_INDEX), values);
    String val = value.toString();

    Assert.assertTrue(WildcardMatcher.match(val, "?\0?\0?\0?\0?"));
    Assert.assertTrue(val.contains("1"));
    Assert.assertTrue(val.contains("2"));
    Assert.assertTrue(val.contains("3"));
    Assert.assertTrue(val.contains("4"));
    Assert.assertTrue(val.contains("5"));
  }

  @Test
  public void testReduceIteratorHavingDuplicates() {

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("2"),
        new Value("3"), new Value("4")).iterator();

    DataStoreHashIndexCombiner combiner = new DataStoreHashIndexCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), TEXT_HASH_INDEX), values);
    String val = value.toString();

    Assert.assertTrue(WildcardMatcher.match(val, "?\0?\0?\0?"));
    Assert.assertTrue(val.contains("1"));
    Assert.assertTrue(val.contains("2"));
    Assert.assertTrue(val.contains("3"));
    Assert.assertTrue(val.contains("4"));
  }
}
