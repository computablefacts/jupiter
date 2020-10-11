package com.computablefacts.jupiter;

import org.junit.Assert;
import org.junit.Test;

public class BloomFiltersTest {

  @Test
  public void testSerializationOneElement() {

    BloomFilters<Integer> bloomFiltersOriginal = new BloomFilters<>();
    bloomFiltersOriginal.put(1);

    String str = BloomFilters.toString(bloomFiltersOriginal);
    BloomFilters<Integer> bloomFiltersReloaded = BloomFilters.fromString(str);

    Assert.assertEquals(bloomFiltersOriginal, bloomFiltersReloaded);
  }

  @Test
  public void testSerializationMoreThanOneElement() {

    BloomFilters<Integer> bloomFiltersOriginal = new BloomFilters<>();

    for (int i = 0; i < 5000000; i++) {
      bloomFiltersOriginal.put(i);
    }

    String str = BloomFilters.toString(bloomFiltersOriginal);
    BloomFilters<Integer> bloomFiltersReloaded = BloomFilters.fromString(str);

    Assert.assertEquals(bloomFiltersOriginal, bloomFiltersReloaded);
  }
}
