package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldLabelsTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLabels fl = new FieldLabels(null, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsAccumulo() {
    FieldLabels fl = new FieldLabels("term", null, Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsTerm() {
    FieldLabels fl = new FieldLabels("term", Sets.newHashSet(), null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldLabels.class).verify();
  }
}
