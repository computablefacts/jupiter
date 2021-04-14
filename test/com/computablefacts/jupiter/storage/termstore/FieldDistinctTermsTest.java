package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldDistinctTermsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldDistinctTerms fdb = new FieldDistinctTerms(null, "field", Term.TYPE_UNKNOWN,
        Sets.newHashSet(), new MySketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldDistinctTerms fdb = new FieldDistinctTerms("dataset", null, Term.TYPE_UNKNOWN,
        Sets.newHashSet(), new MySketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldDistinctTerms fdb = new FieldDistinctTerms("dataset", "field", Term.TYPE_UNKNOWN, null,
        new MySketch().toByteArray());
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldDistinctTerms.class).verify();
  }
}
