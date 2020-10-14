package com.computablefacts.jupiter.storage.blobstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BlobTest {

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    Blob<String> blob = new Blob<>(null, Sets.newHashSet(), "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Blob<String> blob = new Blob<>("key", null, "value");
  }

  @Test(expected = NullPointerException.class)
  public void testNullValue() {
    Blob<String> blob = new Blob<>("key", Sets.newHashSet(), null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Blob.class).verify();
  }
}
