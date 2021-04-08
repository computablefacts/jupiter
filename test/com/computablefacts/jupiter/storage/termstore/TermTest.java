package com.computablefacts.jupiter.storage.termstore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TermTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    Term term = new Term(null, "docId", "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullDocId() {
    Term term = new Term("dataset", null, "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    Term term = new Term("dataset", "docId", null, Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    Term term =
        new Term("dataset", "docId", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Term term = new Term("dataset", "docId", "field", Term.TYPE_STRING, "term", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Term.class).verify();
  }
}
