package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldLabels implements HasField {

  private final String field_;
  private final Set<String> labelsAccumulo_;
  private final Set<String> labelsTerm_;

  public FieldLabels(String field, Set<String> labelsAccumulo, Set<String> labelsTerm) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labelsAccumulo, "labelsAccumulo should not be null");
    Preconditions.checkNotNull(labelsTerm, "labelsTerm should not be null");

    field_ = field;
    labelsAccumulo_ = new HashSet<>(labelsAccumulo);
    labelsTerm_ = new HashSet<>(labelsTerm);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_)
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
    return Objects.equal(field_, term.field_)
        && Objects.equal(labelsAccumulo_, term.labelsAccumulo_)
        && Objects.equal(labelsTerm_, term.labelsTerm_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, labelsAccumulo_, labelsTerm_);
  }

  @Override
  public String field() {
    return field_;
  }

  public Set<String> accumuloLabels() {
    return labelsAccumulo_;
  }

  public Set<String> termLabels() {
    return labelsTerm_;
  }
}
