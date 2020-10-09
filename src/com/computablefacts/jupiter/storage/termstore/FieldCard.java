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
final public class FieldCard implements HasField {

  private final String field_;
  private final Set<String> labels_;
  private final long cardinality_;

  public FieldCard(String field, Set<String> labels, long count) {

    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    field_ = field;
    labels_ = new HashSet<>(labels);
    cardinality_ = count;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_).add("labels", labels_)
        .add("cardinality", cardinality_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof FieldCard)) {
      return false;
    }
    FieldCard term = (FieldCard) obj;
    return Objects.equal(field_, term.field_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(cardinality_, term.cardinality_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field_, labels_, cardinality_);
  }

  @Override
  public String field() {
    return field_;
  }

  public Set<String> labels() {
    return labels_;
  }

  public long cardinality() {
    return cardinality_;
  }
}
