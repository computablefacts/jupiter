package com.computablefacts.jupiter.storage.blobstore;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class BlobTest {

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    Blob<String> blob = new Blob<>(null, Sets.newHashSet(), "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Blob<String> blob = new Blob<>("key", null, "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullValue() {
    Blob<String> blob = new Blob<>("key", Sets.newHashSet(), null);
  }

  @Test
  public void testNull() {

    Blob<String> blob = new Blob<>("key", Sets.newHashSet("label_1", "label_2"), "value");

    Assert.assertFalse(blob.equals(null));
  }

  @Test
  public void testEquals() {

    Blob<String> blob1 = new Blob<>("key", Sets.newHashSet("label_1", "label_2"), "value");
    Blob<String> blob2 = new Blob<>("key", Sets.newHashSet("label_2", "label_1"), "value");

    Assert.assertEquals(blob1, blob2);
    Assert.assertEquals(blob2, blob1);
  }

  @Test
  public void testHashcode() {

    Blob<String> blob1 = new Blob<>("key", Sets.newHashSet("label_1", "label_2"), "value");
    Blob<String> blob2 = new Blob<>("key", Sets.newHashSet("label_2", "label_1"), "value");

    Assert.assertEquals(blob1.hashCode(), blob2.hashCode());
  }

  @Test
  public void testDifferent() {

    Blob<String> blob1 = new Blob<>("key", Sets.newHashSet("label_1", "label_2"), "value");
    Blob<String> blob2 = new Blob<>("key1", Sets.newHashSet("label_1", "label_2"), "value");
    Blob<String> blob3 = new Blob<>("key", Sets.newHashSet("label_1", "label_3"), "value");
    Blob<String> blob4 = new Blob<>("key", Sets.newHashSet("label_1", "label_2"), "value1");

    Assert.assertNotEquals(blob1, blob2);
    Assert.assertNotEquals(blob1, blob3);
    Assert.assertNotEquals(blob1, blob4);
    Assert.assertNotEquals(blob2, blob3);
    Assert.assertNotEquals(blob2, blob4);
    Assert.assertNotEquals(blob3, blob4);

    Assert.assertNotEquals(blob1.hashCode(), blob2.hashCode());
    Assert.assertNotEquals(blob1.hashCode(), blob3.hashCode());
    Assert.assertNotEquals(blob1.hashCode(), blob4.hashCode());
    Assert.assertNotEquals(blob2.hashCode(), blob3.hashCode());
    Assert.assertNotEquals(blob2.hashCode(), blob4.hashCode());
    Assert.assertNotEquals(blob3.hashCode(), blob4.hashCode());
  }
}
