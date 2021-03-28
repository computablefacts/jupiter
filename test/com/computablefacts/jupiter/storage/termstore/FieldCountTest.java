package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldCount fc = new FieldCount(null, Term.TYPE_UNKNOWN, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldCount fc = new FieldCount("key", Term.TYPE_UNKNOWN, null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldCount.class).verify();
  }
}
