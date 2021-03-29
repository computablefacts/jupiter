package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldLabels implements HasField, HasTermType {

  private final String field_;
  private final int termType_;
  private final Set<String> labelsAccumulo_;
  private final Set<String> labelsTerm_;

  public FieldLabels(String field, int termType, Set<String> labelsAccumulo,
      Set<String> labelsTerm) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labelsAccumulo, "labelsAccumulo should not be null");
    Preconditions.checkNotNull(labelsTerm, "labelsTerm should not be null");

    field_ = field;
    termType_ = termType;
    labelsAccumulo_ = new HashSet<>(labelsAccumulo);
    labelsTerm_ = new HashSet<>(labelsTerm);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("term_type", termType_)
        .add("labelsAccumulo", labelsAccumulo_).add("labelsTerm", labelsTerm_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof FieldLabels)) {
      return false;
    }
    FieldLabels term = (FieldLabels) obj;
    return Objects.equal(field_, term.field_) && Objects.equal(termType_, term.termType_)
        && Objects.equal(labelsAccumulo_, term.labelsAccumulo_)
        && Objects.equal(labelsTerm_, term.labelsTerm_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, termType_, labelsAccumulo_, labelsTerm_);
  }

  @Generated
  @Override
  public String field() {
    return field_;
  }

  @Generated
  @Override
  public int termType() {
    return termType_;
  }

  @Generated
  @Override
  public boolean isUnknown() {
    return termType_ == Term.TYPE_UNKNOWN;
  }

  @Generated
  @Override
  public boolean isString() {
    return termType_ == Term.TYPE_STRING;
  }

  @Generated
  @Override
  public boolean isNumber() {
    return termType_ == Term.TYPE_NUMBER;
  }

  @Generated
  @Override
  public boolean isDate() {
    return termType_ == Term.TYPE_DATE;
  }

  @Generated
  public Set<String> accumuloLabels() {
    return labelsAccumulo_;
  }

  @Generated
  public Set<String> termLabels() {
    return labelsTerm_;
  }
}
