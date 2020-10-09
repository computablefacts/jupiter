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

public class TermCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    TermCount tc = new TermCount(null, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    TermCount tc = new TermCount("field", null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    TermCount tc = new TermCount("field", "term", null, 0);
  }

  @Test
  public void testEqualsNull() {

    TermCount tc = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), 1);

    Assert.assertFalse(tc.equals(null));
  }

  @Test
  public void testEqualsPositiveCount() {

    TermCount tc1 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), 1);
    TermCount tc2 = new TermCount("field", "term", Sets.newHashSet("label_2", "label_1"), 1);

    Assert.assertEquals(tc1, tc2);
    Assert.assertEquals(tc2, tc1);
  }

  @Test
  public void testEqualsNegativeCount() {

    TermCount tc1 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), -1);
    TermCount tc2 = new TermCount("field", "term", Sets.newHashSet("label_2", "label_1"), -1);

    Assert.assertEquals(tc1, tc2);
    Assert.assertEquals(tc2, tc1);
  }

  @Test
  public void testHashcodePositiveCount() {

    TermCount tc1 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), 1);
    TermCount tc2 = new TermCount("field", "term", Sets.newHashSet("label_2", "label_1"), 1);

    Assert.assertEquals(tc1.hashCode(), tc2.hashCode());
    Assert.assertEquals(tc2.hashCode(), tc1.hashCode());
  }

  @Test
  public void testHashcodeNegativeCount() {

    TermCount tc1 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), -1);
    TermCount tc2 = new TermCount("field", "term", Sets.newHashSet("label_2", "label_1"), -1);

    Assert.assertEquals(tc1.hashCode(), tc2.hashCode());
    Assert.assertEquals(tc2.hashCode(), tc1.hashCode());
  }

  @Test
  public void testDifferent() {

    TermCount tc1 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), -1);
    TermCount tc2 = new TermCount("field1", "term", Sets.newHashSet("label_1", "label_2"), -1);
    TermCount tc3 = new TermCount("field", "term1", Sets.newHashSet("label_1", "label_2"), -1);
    TermCount tc4 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_3"), -1);
    TermCount tc5 = new TermCount("field", "term", Sets.newHashSet("label_1", "label_2"), 1);

    Assert.assertNotEquals(tc1, tc2);
    Assert.assertNotEquals(tc1, tc3);
    Assert.assertNotEquals(tc1, tc4);
    Assert.assertNotEquals(tc1, tc5);
    Assert.assertNotEquals(tc2, tc3);
    Assert.assertNotEquals(tc2, tc4);
    Assert.assertNotEquals(tc2, tc5);
    Assert.assertNotEquals(tc3, tc4);
    Assert.assertNotEquals(tc3, tc5);
    Assert.assertNotEquals(tc4, tc5);

    Assert.assertNotEquals(tc1.hashCode(), tc2.hashCode());
    Assert.assertNotEquals(tc1.hashCode(), tc3.hashCode());
    Assert.assertNotEquals(tc1.hashCode(), tc4.hashCode());
    Assert.assertNotEquals(tc1.hashCode(), tc5.hashCode());
    Assert.assertNotEquals(tc2.hashCode(), tc3.hashCode());
    Assert.assertNotEquals(tc2.hashCode(), tc4.hashCode());
    Assert.assertNotEquals(tc2.hashCode(), tc5.hashCode());
    Assert.assertNotEquals(tc3.hashCode(), tc4.hashCode());
    Assert.assertNotEquals(tc3.hashCode(), tc5.hashCode());
    Assert.assertNotEquals(tc4.hashCode(), tc5.hashCode());
  }
}
