package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldTopTermsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldTopTerms ftt = new FieldTopTerms(null, "field", Term.TYPE_UNKNOWN, Sets.newHashSet(),
        new ThetaSketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldTopTerms ftt = new FieldTopTerms("dataset", null, Term.TYPE_UNKNOWN, Sets.newHashSet(),
        new ThetaSketch().toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldTopTerms ftt = new FieldTopTerms("dataset", "field", Term.TYPE_UNKNOWN, null,
        new ThetaSketch().toByteArray());
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldTopTerms.class).verify();
  }
}
