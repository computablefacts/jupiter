package com.computablefacts.jupiter.storage.blobstore;

import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
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

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key_).add("labels", labels_)
        .add("value", value_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
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

  @Generated
  public String key() {
    return key_;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public T value() {
    return value_;
  }
}
