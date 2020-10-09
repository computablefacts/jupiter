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

import org.apache.accumulo.core.util.ComparablePair;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TermTest {

  @Test(expected = NullPointerException.class)
  public void testNullDocId() {
    Term term = new Term(null, "field", "term", Sets.newHashSet(), 0, Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    Term term = new Term("docId", null, "term", Sets.newHashSet(), 0, Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    Term term = new Term("docId", "field", null, Sets.newHashSet(), 0, Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Term term = new Term("docId", "field", "term", null, 0, Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullSpans() {
    Term term = new Term("docId", "field", "term", Sets.newHashSet(), 0, null);
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchBetweenCountAndSpans() {
    Term term = new Term("docId", "field", "term", Sets.newHashSet(), 1, Lists.newArrayList());
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchBetweenSpansAndCount() {
    Term term = new Term("docId", "field", "term", Sets.newHashSet(), 0,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
  }

  @Test
  public void testEqualsNull() {

    Term t = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));

    Assert.assertFalse(t.equals(null));
  }

  @Test
  public void testEquals() {

    Term t1 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t2 = new Term("docId", "field", "term", Sets.newHashSet("label_2", "label_1"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));

    Assert.assertEquals(t1, t2);
    Assert.assertEquals(t2, t1);
  }

  @Test
  public void testHashcode() {

    Term t1 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t2 = new Term("docId", "field", "term", Sets.newHashSet("label_2", "label_1"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));

    Assert.assertEquals(t1.hashCode(), t2.hashCode());
  }

  @Test
  public void testCompare() {

    Term t1 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 2,
        Lists.newArrayList(new ComparablePair<>(1, 5), new ComparablePair<>(7, 11)));
    Term t2 = new Term("docId", "field", "term", Sets.newHashSet("label_2", "label_1"), 2,
        Lists.newArrayList(new ComparablePair<>(7, 11), new ComparablePair<>(1, 5)));

    Assert.assertEquals(0, t1.compareTo(t2));
  }

  @Test
  public void testDifferent() {

    Term t1 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t2 = new Term("docId1", "field", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t3 = new Term("docId", "field1", "term", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t4 = new Term("docId", "field", "term1", Sets.newHashSet("label_1", "label_2"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t5 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_3"), 1,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
    Term t6 = new Term("docId", "field", "term", Sets.newHashSet("label_1", "label_2"), 0,
        Lists.newArrayList());

    Assert.assertNotEquals(t1, t2);
    Assert.assertNotEquals(t1, t3);
    Assert.assertNotEquals(t1, t4);
    Assert.assertNotEquals(t1, t5);
    Assert.assertNotEquals(t1, t6);
    Assert.assertNotEquals(t2, t3);
    Assert.assertNotEquals(t2, t4);
    Assert.assertNotEquals(t2, t5);
    Assert.assertNotEquals(t2, t6);
    Assert.assertNotEquals(t3, t4);
    Assert.assertNotEquals(t3, t5);
    Assert.assertNotEquals(t3, t6);
    Assert.assertNotEquals(t4, t5);
    Assert.assertNotEquals(t4, t6);
    Assert.assertNotEquals(t5, t6);

    Assert.assertNotEquals(t1.hashCode(), t2.hashCode());
    Assert.assertNotEquals(t1.hashCode(), t3.hashCode());
    Assert.assertNotEquals(t1.hashCode(), t4.hashCode());
    Assert.assertNotEquals(t1.hashCode(), t5.hashCode());
    Assert.assertNotEquals(t1.hashCode(), t6.hashCode());
    Assert.assertNotEquals(t2.hashCode(), t3.hashCode());
    Assert.assertNotEquals(t2.hashCode(), t4.hashCode());
    Assert.assertNotEquals(t2.hashCode(), t5.hashCode());
    Assert.assertNotEquals(t2.hashCode(), t6.hashCode());
    Assert.assertNotEquals(t3.hashCode(), t4.hashCode());
    Assert.assertNotEquals(t3.hashCode(), t5.hashCode());
    Assert.assertNotEquals(t3.hashCode(), t6.hashCode());
    Assert.assertNotEquals(t4.hashCode(), t5.hashCode());
    Assert.assertNotEquals(t4.hashCode(), t6.hashCode());
    Assert.assertNotEquals(t5.hashCode(), t6.hashCode());
  }
}
