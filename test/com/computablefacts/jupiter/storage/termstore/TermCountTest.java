package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TermCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    TermCount tc = new TermCount(null, "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    TermCount tc = new TermCount("dataset", null, Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    TermCount tc = new TermCount("dataset", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    TermCount tc = new TermCount("dataset", "field", Term.TYPE_STRING, "term", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(TermCount.class).verify();
  }
}
