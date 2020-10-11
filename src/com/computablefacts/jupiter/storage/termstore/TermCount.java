package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class TermCount implements HasField, HasTerm {

  private final String field_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;

  public TermCount(String field, String term, Set<String> labels, long count) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    field_ = field;
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("term", term_)
        .add("labels", labels_).add("count", count_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TermCount)) {
      return false;
    }
    TermCount term = (TermCount) obj;
    return Objects.equal(field_, term.field_) && Objects.equal(term_, term.term_)
        && Objects.equal(labels_, term.labels_) && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, term_, labels_, count_);
  }

  @Override
  public String field() {
    return field_;
  }

  @Override
  public String term() {
    return term_;
  }

  public Set<String> labels() {
    return labels_;
  }

  public long count() {
    return count_;
  }
}
