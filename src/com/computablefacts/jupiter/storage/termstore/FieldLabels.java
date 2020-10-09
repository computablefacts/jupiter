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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("field", field_)
        .add("labelsAccumulo", labelsAccumulo_).add("labelsTerm", labelsTerm_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
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
