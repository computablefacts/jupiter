package com.computablefacts.jupiter.storage.termstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.util.ComparablePair;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Term implements HasTerm, HasTermType, Comparable<Term> {

  public static final int TYPE_UNKNOWN = 0;
  public static final int TYPE_STRING = 1;
  public static final int TYPE_NUMBER = 2;
  public static final int TYPE_DATE = 3;

  private final String docId_;
  private final String field_;
  private final int termType_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;
  private final List<ComparablePair<Integer, Integer>> spans_;

  public Term(String docId, String field, int termType, String term, Set<String> labels, long count,
      List<ComparablePair<Integer, Integer>> spans) {

    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(spans, "spans should not be null");

    Preconditions.checkState(count == spans.size(),
        "inconsistent state for (field, term, count, spans) : (%s, %s, %s, %s)", field, term, count,
        spans);

    docId_ = docId;
    field_ = field;
    termType_ = termType;
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
    spans_ = new ArrayList<>(spans);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("docId", docId_).add("field", field_)
        .add("term", term_).add("term_type", termType_).add("labels", labels_).add("count", count_)
        .add("spans", spans_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Term)) {
      return false;
    }
    Term term = (Term) obj;
    return Objects.equal(docId_, term.docId_) && Objects.equal(field_, term.field_)
        && Objects.equal(termType_, term.termType_) && Objects.equal(term_, term.term_)
        && Objects.equal(labels_, term.labels_) && Objects.equal(count_, term.count_)
        && Objects.equal(spans_, term.spans_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(docId_, field_, termType_, term_, labels_, count_, spans_);
  }

  @Override
  public int compareTo(@NotNull Term term) {

    @Var
    int cmp = ComparisonChain.start().compare(docId_, term.docId_).compare(field_, term.field_)
        .compare(termType_, term.termType_).compare(term_, term.term_).compare(count_, term.count_)
        .result();

    if (cmp != 0) {
      return cmp;
    }

    cmp = compare(labels_, term.labels_);

    if (cmp != 0) {
      return cmp;
    }
    return compare(spans_, term.spans_);
  }

  @Generated
  @Override
  public int termType() {
    return termType_;
  }

  @Generated
  @Override
  public String term() {
    return term_;
  }

  @Generated
  @Override
  public boolean isUnknown() {
    return termType_ == TYPE_UNKNOWN;
  }

  @Generated
  @Override
  public boolean isString() {
    return termType_ == TYPE_STRING;
  }

  @Generated
  @Override
  public boolean isNumber() {
    return termType_ == TYPE_NUMBER;
  }

  @Generated
  @Override
  public boolean isDate() {
    return termType_ == TYPE_DATE;
  }

  @Generated
  public String docId() {
    return docId_;
  }

  @Generated
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

  @Generated
  public List<ComparablePair<Integer, Integer>> spans() {
    return spans_;
  }

  private <T extends Comparable<T>> int compare(Collection<T> l1, Collection<T> l2) {

    @Var
    int cmp = Integer.compare(l1.size(), l2.size());

    if (cmp != 0) {
      return cmp;
    }

    List<T> tmp1 = Lists.newArrayList(l1);
    List<T> tmp2 = Lists.newArrayList(l2);

    Collections.sort(tmp1);
    Collections.sort(tmp2);

    for (int i = 0; i < tmp1.size(); i++) {

      cmp = tmp1.get(i).compareTo(tmp2.get(i));

      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }
}
