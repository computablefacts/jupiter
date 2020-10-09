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

import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class GroupByTermIteratorTest {

  @Test(expected = NullPointerException.class)
  public void testGroupByNullCollection() {
    Iterator<?> iterator = new GroupByTermIterator<>(null);
  }

  @Test
  public void testGroupByEmptyCollection() {

    List<?> listComputed =
        Lists.newArrayList(new GroupByTermIterator(Lists.newArrayList().iterator()));

    List<?> listExpected = Lists.newArrayList();

    Assert.assertEquals(0, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testGroupByCollectionWithoutDuplicates() {

    List<Pair<String, List<Term>>> listComputed =
        Lists.newArrayList(new GroupByTermIterator<>(listWithoutDuplicates()));

    List<Pair<String, List<Term>>> listExpected = pairsWithoutDuplicates();

    Assert.assertEquals(3, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testGroupByCollectionWithDuplicates() {

    List<Pair<String, List<Term>>> listComputed =
        Lists.newArrayList(new GroupByTermIterator<>(listWithDuplicates()));

    List<Pair<String, List<Term>>> listExpected = pairsWithDuplicates();

    Assert.assertEquals(3, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  private Iterator<Term> listWithoutDuplicates() {
    return Lists.newArrayList(new Term("1"), new Term("2"), new Term("3")).iterator();
  }

  private List<Pair<String, List<Term>>> pairsWithoutDuplicates() {
    return Lists.newArrayList(new Pair<>("1", Lists.newArrayList(new Term("1"))),
        new Pair<>("2", Lists.newArrayList(new Term("2"))),
        new Pair<>("3", Lists.newArrayList(new Term("3"))));
  }

  private Iterator<Term> listWithDuplicates() {
    return Lists.newArrayList(new Term("1"), new Term("1"), new Term("2"), new Term("3"),
        new Term("3"), new Term("3")).iterator();
  }

  private List<Pair<String, List<Term>>> pairsWithDuplicates() {
    return Lists.newArrayList(new Pair<>("1", Lists.newArrayList(new Term("1"), new Term("1"))),
        new Pair<>("2", Lists.newArrayList(new Term("2"))),
        new Pair<>("3", Lists.newArrayList(new Term("3"), new Term("3"), new Term("3"))));
  }

  private static class Term implements HasTerm {

    private final String term_;

    public Term(String term) {
      term_ = Strings.nullToEmpty(term);
    }

    @Override
    public String term() {
      return term_;
    }

    @Override
    public String toString() {
      return "term=" + term_;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof Term)) {
        return false;
      }
      return term_.equals(((Term) obj).term_);
    }

    @Override
    public int hashCode() {
      return term_.hashCode();
    }
  }
}
