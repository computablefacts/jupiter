package com.computablefacts.jupiter.storage.termstore;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldLastUpdateTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLastUpdate fl = new FieldLastUpdate(null, Term.TYPE_UNKNOWN, Sets.newHashSet(),
        DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldLastUpdate fl = new FieldLastUpdate("key", Term.TYPE_UNKNOWN, null,
        DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldLastUpdate.class).verify();
  }
}
