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

import java.util.HashSet;
import java.util.Set;

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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("term", term_)
        .add("labels", labels_).add("cardinality", cardinality_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
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

  public long cardinality() {
    return cardinality_;
  }
}
