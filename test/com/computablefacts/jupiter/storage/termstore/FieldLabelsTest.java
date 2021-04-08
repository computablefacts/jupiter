package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldLabelsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldLabels fl =
        new FieldLabels(null, "term", Term.TYPE_UNKNOWN, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLabels fl =
        new FieldLabels("dataset", null, Term.TYPE_UNKNOWN, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsAccumulo() {
    FieldLabels fl = new FieldLabels("dataset", "term", Term.TYPE_UNKNOWN, null, Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsTerm() {
    FieldLabels fl = new FieldLabels("dataset", "term", Term.TYPE_UNKNOWN, Sets.newHashSet(), null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldLabels.class).verify();
  }
}
