/*
 * Copyright (c) 2011-2020 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.termstore.ComparablePair.compare;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.util.ComparablePair;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Term implements HasTerm, Comparable<Term> {

  private final String docId_;
  private final String field_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;
  private final List<ComparablePair<Integer, Integer>> spans_;

  public Term(String docId, String field, String term, Set<String> labels, long count,
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
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
    spans_ = new ArrayList<>(spans);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("docId", docId_).add("field", field_)
        .add("term", term_).add("labels", labels_).add("count", count_).add("spans", spans_)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Term)) {
      return false;
    }
    Term term = (Term) obj;
    return Objects.equal(docId_, term.docId_) && Objects.equal(field_, term.field_)
        && Objects.equal(term_, term.term_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(count_, term.count_) && Objects.equal(spans_, term.spans_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(docId_, field_, term_, labels_, count_, spans_);
  }

  @Override
  public int compareTo(@NotNull Term term) {

    @Var
    int cmp = ComparisonChain.start().compare(docId_, term.docId_).compare(field_, term.field_)
        .compare(term_, term.term_).compare(count_, term.count_).result();

    if (cmp != 0) {
      return cmp;
    }

    cmp = compare(labels_, term.labels_);

    if (cmp != 0) {
      return cmp;
    }
    return compare(spans_, term.spans_);
  }

  @Override
  public String term() {
    return term_;
  }

  public String docId() {
    return docId_;
  }

  public String field() {
    return field_;
  }

  public Set<String> labels() {
    return labels_;
  }

  public long count() {
    return count_;
  }

  public List<ComparablePair<Integer, Integer>> spans() {
    return spans_;
  }
}
