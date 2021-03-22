package com.computablefacts.jupiter.storage.blobstore;

import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BlobTest {

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    Blob<String> blob = new Blob<>(null, Sets.newHashSet(), 0, Lists.newArrayList(), "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Blob<String> blob = new Blob<>("key", null, 0, Lists.newArrayList(), "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperties() {
    Blob<String> blob = new Blob<>("key", Sets.newHashSet(), 0, null, "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullValue() {
    Blob<String> blob = new Blob<>("key", Sets.newHashSet(), 0, Lists.newArrayList(), null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Blob.class).verify();
  }
}
