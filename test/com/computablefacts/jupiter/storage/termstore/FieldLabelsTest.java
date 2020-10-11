package com.computablefacts.jupiter.storage.termstore;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class FieldLabelsTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLabels fl = new FieldLabels(null, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsAccumulo() {
    FieldLabels fl = new FieldLabels("term", null, Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsTerm() {
    FieldLabels fl = new FieldLabels("term", Sets.newHashSet(), null);
  }

  @Test
  public void testEqualsNull() {

    FieldLabels fl =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_3"));

    Assert.assertFalse(fl.equals(null));
  }

  @Test
  public void testEquals() {

    FieldLabels fl1 =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_3"));
    FieldLabels fl2 =
        new FieldLabels("term", Sets.newHashSet("label_2", "label_1"), Sets.newHashSet("label_3"));

    Assert.assertEquals(fl1, fl2);
    Assert.assertEquals(fl2, fl1);
  }

  @Test
  public void testHashcode() {

    FieldLabels fl1 =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_3"));
    FieldLabels fl2 =
        new FieldLabels("term", Sets.newHashSet("label_2", "label_1"), Sets.newHashSet("label_3"));

    Assert.assertEquals(fl1.hashCode(), fl2.hashCode());
  }

  @Test
  public void testDifferent() {

    FieldLabels fl1 =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_3"));
    FieldLabels fl2 =
        new FieldLabels("term1", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_3"));
    FieldLabels fl3 =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_3"), Sets.newHashSet("label_3"));
    FieldLabels fl4 =
        new FieldLabels("term", Sets.newHashSet("label_1", "label_2"), Sets.newHashSet("label_4"));

    Assert.assertNotEquals(fl1, fl2);
    Assert.assertNotEquals(fl1, fl3);
    Assert.assertNotEquals(fl1, fl4);
    Assert.assertNotEquals(fl2, fl3);
    Assert.assertNotEquals(fl2, fl4);
    Assert.assertNotEquals(fl3, fl4);

    Assert.assertNotEquals(fl1.hashCode(), fl2.hashCode());
    Assert.assertNotEquals(fl1.hashCode(), fl3.hashCode());
    Assert.assertNotEquals(fl1.hashCode(), fl4.hashCode());
    Assert.assertNotEquals(fl2.hashCode(), fl3.hashCode());
    Assert.assertNotEquals(fl2.hashCode(), fl4.hashCode());
    Assert.assertNotEquals(fl3.hashCode(), fl4.hashCode());
  }
}
