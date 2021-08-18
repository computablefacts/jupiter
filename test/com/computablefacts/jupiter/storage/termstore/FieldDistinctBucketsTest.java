package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldDistinctBucketsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldDistinctBuckets fdb =
        new FieldDistinctBuckets(null, "field", Term.TYPE_NA, Sets.newHashSet(), 1);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldDistinctBuckets fdb =
        new FieldDistinctBuckets("dataset", null, Term.TYPE_NA, Sets.newHashSet(), 1);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldDistinctBuckets fdb = new FieldDistinctBuckets("dataset", "field", Term.TYPE_NA, null, 1);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldDistinctBuckets.class).verify();
  }
}
