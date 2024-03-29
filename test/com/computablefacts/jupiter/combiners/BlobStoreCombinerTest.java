package com.computablefacts.jupiter.combiners;

import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_ARRAY;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_STRING;

import com.computablefacts.asterix.WildcardMatcher;
import com.google.common.collect.Lists;
import java.util.Iterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class BlobStoreCombinerTest {

  @Test
  public void testReduceNotTheRightColumnFamily() {

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("3"), new Value("4"),
        new Value("5")).iterator();

    BlobStoreCombiner combiner = new BlobStoreCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), new Text(TYPE_STRING)), values);

    Assert.assertEquals(VALUE_EMPTY, value);
  }

  @Test
  public void testReduceIteratorHavingNoDuplicates() {

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("3"), new Value("4"),
        new Value("5")).iterator();

    BlobStoreCombiner combiner = new BlobStoreCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), new Text(TYPE_ARRAY)), values);
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

    Iterator<Value> values = Lists.newArrayList(new Value("1"), new Value("2"), new Value("2"), new Value("3"),
        new Value("4")).iterator();

    BlobStoreCombiner combiner = new BlobStoreCombiner();
    Value value = combiner.reduce(new Key(new Text("id"), new Text(TYPE_ARRAY)), values);
    String val = value.toString();

    Assert.assertTrue(WildcardMatcher.match(val, "?\0?\0?\0?"));
    Assert.assertTrue(val.contains("1"));
    Assert.assertTrue(val.contains("2"));
    Assert.assertTrue(val.contains("3"));
    Assert.assertTrue(val.contains("4"));
  }
}
