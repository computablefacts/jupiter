package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TermCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    TermCount tc = new TermCount(null, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    TermCount tc = new TermCount("field", null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    TermCount tc = new TermCount("field", "term", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(TermCount.class).verify();
  }
}
