package com.computablefacts.jupiter;

import org.junit.Assert;
import org.junit.Test;

public class BloomFiltersTest {

  @Test
  public void testInitWithNull() {

    BloomFilters<Integer> bfOriginal = new BloomFilters<>(null);
    BloomFilters<Integer> bfNew = new BloomFilters<>(bfOriginal);

    Assert.assertEquals(bfOriginal, bfNew);

    String strOriginal = BloomFilters.toString(bfOriginal);
    String strNew = BloomFilters.toString(bfNew);

    Assert.assertEquals(strOriginal, strNew);
    Assert.assertEquals(2, strNew.length());
    Assert.assertEquals("0¤", strNew);
  }

  @Test
  public void testInitWithAnotherElement() {

    BloomFilters<Integer> bfOriginal = new BloomFilters<>();
    bfOriginal.put(1);

    BloomFilters<Integer> bfNew = new BloomFilters<>(bfOriginal);

    Assert.assertEquals(bfOriginal, bfNew);

    String strOriginal = BloomFilters.toString(bfOriginal);
    String strNew = BloomFilters.toString(bfNew);

    Assert.assertEquals(strOriginal, strNew);
    Assert.assertEquals(721356, strNew.length());
    Assert.assertTrue(strNew.startsWith("1¤1¤"));
  }

  @Test
  public void testSerializeOneElement() {

    BloomFilters<Integer> bloomFiltersOriginal = new BloomFilters<>();
    bloomFiltersOriginal.put(1);

    String str = BloomFilters.toString(bloomFiltersOriginal);
    BloomFilters<Integer> bloomFiltersReloaded = BloomFilters.fromString(str);

    Assert.assertEquals(bloomFiltersOriginal, bloomFiltersReloaded);
  }

  @Test
  public void testSerializeMoreThanOneElement() {

    BloomFilters<Integer> bloomFiltersOriginal = new BloomFilters<>();

    for (int i = 0; i < 5000000; i++) {
      bloomFiltersOriginal.put(i);
    }

    String str = BloomFilters.toString(bloomFiltersOriginal);
    BloomFilters<Integer> bloomFiltersReloaded = BloomFilters.fromString(str);

    Assert.assertEquals(bloomFiltersOriginal, bloomFiltersReloaded);
  }
}
