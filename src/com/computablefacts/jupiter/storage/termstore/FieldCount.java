package com.computablefacts.jupiter.storage.termstore;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldCount {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final long count_;

  FieldCount(String dataset, String field, int type, Set<String> labels, long count) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = type;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  public static FieldCount fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract term and term's type from ROW
    int index = row.indexOf(Constants.SEPARATOR_NUL);

    String field;
    int type;

    if (index < 0) {
      field = row;
      type = Term.TYPE_UNKNOWN;
    } else {
      field = row.substring(0, index);
      type = Integer.parseInt(row.substring(index + 1), 10);
    }

    // Extract dataset from CF
    String datazet = cf.substring(0, cf.lastIndexOf('_'));

    // Extract visibility labels from CV
    Set<String> labels = Sets.newHashSet(
        Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract term count from VALUE
    long count = Long.parseLong(val, 10);

    return new FieldCount(datazet, field, type, labels, count);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("field", field_)
        .add("type", type_).add("labels", labels_).add("count", count_).toString();
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
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(field_, term.field_)
        && Objects.equal(type_, term.type_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, field_, type_, labels_, count_);
  }

  @Generated
  public String dataset() {
    return dataset_;
  }

  @Generated
  public String field() {
    return field_;
  }

  @Generated
  public int termType() {
    return type_;
  }

  @Generated
  public boolean isUnknown() {
    return type_ == Term.TYPE_UNKNOWN;
  }

  @Generated
  public boolean isString() {
    return type_ == Term.TYPE_STRING;
  }

  @Generated
  public boolean isNumber() {
    return type_ == Term.TYPE_NUMBER;
  }

  @Generated
  public boolean isDate() {
    return type_ == Term.TYPE_DATE;
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
