package com.computablefacts.jupiter.storage.termstore;

import org.apache.accumulo.core.util.ComparablePair;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TermTest {

  @Test(expected = NullPointerException.class)
  public void testNullDocId() {
    Term term = new Term(null, "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0,
        Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    Term term = new Term("docId", null, Term.TYPE_STRING, "term", Sets.newHashSet(), 0,
        Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    Term term = new Term("docId", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet(), 0,
        Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Term term = new Term("docId", "field", Term.TYPE_STRING, "term", null, 0, Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullSpans() {
    Term term = new Term("docId", "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0, null);
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchBetweenCountAndSpans() {
    Term term = new Term("docId", "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 1,
        Lists.newArrayList());
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchBetweenSpansAndCount() {
    Term term = new Term("docId", "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0,
        Lists.newArrayList(new ComparablePair<>(1, 5)));
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Term.class).verify();
  }
}
