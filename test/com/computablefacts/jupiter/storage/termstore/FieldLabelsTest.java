/*
 * Copyright (c) 2011-2020 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
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
