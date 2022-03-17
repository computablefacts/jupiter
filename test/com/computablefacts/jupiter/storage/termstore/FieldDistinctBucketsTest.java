package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

public class FieldDistinctBucketsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets(null, "field", 1);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets("dataset", null, 1);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldDistinctBuckets.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
}
