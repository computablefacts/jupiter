package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldCount implements HasField, HasTermType {

  private final String field_;
  private final int termType_;
  private final Set<String> labels_;
  private final long count_;

  public FieldCount(String field, int termType, Set<String> labels, long count) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    field_ = field;
    termType_ = termType;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("term_type", termType_)
        .add("labels", labels_).add("count", count_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof FieldCount)) {
      return false;
    }
    FieldCount term = (FieldCount) obj;
    return Objects.equal(field_, term.field_) && Objects.equal(termType_, term.termType_)
        && Objects.equal(labels_, term.labels_) && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, termType_, labels_, count_);
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
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public long count() {
    return count_;
  }
}
