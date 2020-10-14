package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class TermCard implements HasField, HasTerm {

  private final String field_;
  private final String term_;
  private final Set<String> labels_;
  private final long cardinality_;

  public TermCard(String field, String term, Set<String> labels, long cardinality) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    field_ = field;
    term_ = term;
    labels_ = new HashSet<>(labels);
    cardinality_ = cardinality;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("term", term_)
        .add("labels", labels_).add("cardinality", cardinality_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TermCard)) {
      return false;
    }
    TermCard term = (TermCard) obj;
    return Objects.equal(field_, term.field_) && Objects.equal(term_, term.term_)
        && Objects.equal(labels_, term.labels_) && Objects.equal(cardinality_, term.cardinality_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, term_, labels_, cardinality_);
  }

  @Generated
  @Override
  public String field() {
    return field_;
  }

  @Generated
  @Override
  public String term() {
    return term_;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public long cardinality() {
    return cardinality_;
  }
}
