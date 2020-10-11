package com.computablefacts.jupiter.storage.termstore;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class FieldCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldCount fc = new FieldCount(null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldCount fc = new FieldCount("key", null, 0);
  }

  @Test
  public void testEqualsNull() {

    FieldCount fc = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), 1);

    Assert.assertFalse(fc.equals(null));
  }

  @Test
  public void testEqualsPositiveCount() {

    FieldCount fc1 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), 1);
    FieldCount fc2 = new FieldCount("key", Sets.newHashSet("label_2", "label_1"), 1);

    Assert.assertEquals(fc1, fc2);
    Assert.assertEquals(fc2, fc1);
  }

  @Test
  public void testEqualsNegativeCount() {

    FieldCount fc1 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), -1);
    FieldCount fc2 = new FieldCount("key", Sets.newHashSet("label_2", "label_1"), -1);

    Assert.assertEquals(fc1, fc2);
    Assert.assertEquals(fc2, fc1);
  }

  @Test
  public void testHashcodePositiveCount() {

    FieldCount fc1 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), 1);
    FieldCount fc2 = new FieldCount("key", Sets.newHashSet("label_2", "label_1"), 1);

    Assert.assertEquals(fc1.hashCode(), fc2.hashCode());
  }

  @Test
  public void testHashcodeNegativeCount() {

    FieldCount fc1 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), -1);
    FieldCount fc2 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), -1);

    Assert.assertEquals(fc1.hashCode(), fc2.hashCode());
  }

  @Test
  public void testDifferent() {

    FieldCount fc1 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), -1);
    FieldCount fc2 = new FieldCount("key1", Sets.newHashSet("label_1", "label_2"), -1);
    FieldCount fc3 = new FieldCount("key", Sets.newHashSet("label_1", "label_3"), -1);
    FieldCount fc4 = new FieldCount("key", Sets.newHashSet("label_1", "label_2"), 1);

    Assert.assertNotEquals(fc1, fc2);
    Assert.assertNotEquals(fc1, fc3);
    Assert.assertNotEquals(fc1, fc4);
    Assert.assertNotEquals(fc2, fc3);
    Assert.assertNotEquals(fc2, fc4);
    Assert.assertNotEquals(fc3, fc4);

    Assert.assertNotEquals(fc1.hashCode(), fc2.hashCode());
    Assert.assertNotEquals(fc1.hashCode(), fc3.hashCode());
    Assert.assertNotEquals(fc1.hashCode(), fc4.hashCode());
    Assert.assertNotEquals(fc2.hashCode(), fc3.hashCode());
    Assert.assertNotEquals(fc2.hashCode(), fc4.hashCode());
    Assert.assertNotEquals(fc3.hashCode(), fc4.hashCode());
  }
}
