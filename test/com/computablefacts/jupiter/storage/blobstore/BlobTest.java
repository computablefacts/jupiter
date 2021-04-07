package com.computablefacts.jupiter.storage.blobstore;

import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BlobTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    Blob<String> blob =
        new Blob<>(null, "key", Sets.newHashSet(), 0, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    Blob<String> blob =
        new Blob<>("dataset", null, Sets.newHashSet(), 0, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Blob<String> blob = new Blob<>("dataset", "key", null, 0, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperties() {
    Blob<String> blob = new Blob<>("dataset", "key", Sets.newHashSet(), 0, "value", null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullValue() {
    Blob<String> blob =
        new Blob<>("dataset", "key", Sets.newHashSet(), 0, null, Lists.newArrayList());
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Blob.class).verify();
  }
}
