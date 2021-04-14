package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldDistinctBucketsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets(null, "field", Term.TYPE_UNKNOWN,
        Sets.newHashSet(), new MySketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets("dataset", null, Term.TYPE_UNKNOWN,
        Sets.newHashSet(), new MySketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets("dataset", "field", Term.TYPE_UNKNOWN, null,
        new MySketch().toByteArray());
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldDistinctBuckets.class).verify();
  }
}
