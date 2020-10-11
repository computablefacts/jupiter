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
