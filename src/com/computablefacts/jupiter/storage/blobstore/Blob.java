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
package com.computablefacts.jupiter.storage.blobstore;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Blob<T> {

  private final String key_;
  private final Set<String> labels_;
  private final T value_;

  public Blob(String key, Set<String> labels, T value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    key_ = key;
    labels_ = new HashSet<>(labels);
    value_ = value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key_).add("labels", labels_)
        .add("value", value_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Blob)) {
      return false;
    }
    Blob<?> blob = (Blob<?>) obj;
    return Objects.equal(key_, blob.key_) && Objects.equal(labels_, blob.labels_)
        && Objects.equal(value_, blob.value_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key_, labels_, value_);
  }

  public String key() {
    return key_;
  }

  public Set<String> labels() {
    return labels_;
  }

  public T value() {
    return value_;
  }
}
