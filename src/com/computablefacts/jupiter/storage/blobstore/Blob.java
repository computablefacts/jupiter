package com.computablefacts.jupiter.storage.blobstore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Blob<T> {

  public static final int TYPE_UNKNOWN = 0;
  public static final int TYPE_STRING = 1;
  public static final int TYPE_FILE = 2;
  public static final int TYPE_JSON = 3;

  private final String key_;
  private final Set<String> labels_;
  private final int type_;
  private final List<String> properties_;
  private final T value_;

  public Blob(String key, Set<String> labels, int type, List<String> properties, T value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(properties, "properties should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    key_ = key;
    labels_ = new HashSet<>(labels);
    type_ = type;
    properties_ = new ArrayList<>(properties);
    value_ = value;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key_).add("labels", labels_)
        .add("type", type_).add("properties", properties_).add("value", value_).toString();
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
        && Objects.equal(type_, blob.type_) && Objects.equal(properties_, blob.properties_)
        && Objects.equal(value_, blob.value_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key_, labels_, type_, properties_, value_);
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
  public int type() {
    return type_;
  }

  @Generated
  public List<String> properties() {
    return properties_;
  }

  @Generated
  public T value() {
    return value_;
  }

  @Generated
  public boolean isUnknown() {
    return type_ == TYPE_UNKNOWN;
  }

  @Generated
  public boolean isString() {
    return type_ == TYPE_STRING;
  }

  @Generated
  public boolean isJson() {
    return type_ == TYPE_JSON;
  }

  @Generated
  public boolean isFile() {
    return type_ == TYPE_FILE;
  }
}
