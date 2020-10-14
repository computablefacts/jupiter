package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldCardTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldCard fc = new FieldCard(null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldCard fc = new FieldCard("key", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldCard.class).verify();
  }
}
