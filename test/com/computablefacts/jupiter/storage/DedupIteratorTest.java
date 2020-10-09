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
package com.computablefacts.jupiter.storage;

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class DedupIteratorTest {

  @Test(expected = NullPointerException.class)
  public void testDedupNullCollection() {
    Iterator<String> iterator = new DedupIterator<>(null);
  }

  @Test
  public void testDedupEmptyCollection() {

    List<String> listComputed =
        Lists.newArrayList(new DedupIterator<>(Lists.<String>newArrayList().iterator()));
    List<String> listExpected = Lists.newArrayList();

    Assert.assertEquals(0, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testDedupCollectionWithoutDuplicates() {

    List<String> listComputed = Lists.newArrayList(new DedupIterator<>(listWithoutDuplicates()));
    List<String> listExpected = Lists.newArrayList(this::listWithoutDuplicates);

    Assert.assertEquals(8, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  @Test
  public void testDedupCollectionWithDuplicates() {

    List<String> listComputed = Lists.newArrayList(new DedupIterator<>(listWithDuplicates()));
    List<String> listExpected = Lists.newArrayList(this::listWithoutDuplicates);

    Assert.assertEquals(8, listComputed.size());
    Assert.assertEquals(listExpected, listComputed);
  }

  private Iterator<String> listWithDuplicates() {
    return Lists.newArrayList("a", "a", "b", "b", "b", "c", "c", "c", "c", "d", "e", "f", "g", "h")
        .iterator();
  }

  private Iterator<String> listWithoutDuplicates() {
    return Lists.newArrayList("a", "b", "c", "d", "e", "f", "g", "h").iterator();
  }
}
