package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldCount implements HasField {

  private final String field_;
  private final Set<String> labels_;
  private final long count_;

  public FieldCount(String field, Set<String> labels, long count) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    field_ = field;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("labels", labels_)
        .add("count", count_).toString();
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
    return Objects.equal(field_, term.field_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, labels_, count_);
  }

  @Generated
  @Override
  public String field() {
    return field_;
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
