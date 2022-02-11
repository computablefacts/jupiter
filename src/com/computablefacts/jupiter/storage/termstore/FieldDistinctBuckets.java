package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.*;
import static com.computablefacts.jupiter.storage.termstore.Term.TYPE_NA;
import static com.computablefacts.jupiter.storage.termstore.TermStore.DISTINCT_BUCKETS;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.asterix.Generated;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@Beta
@CheckReturnValue
final public class FieldDistinctBuckets {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final long estimate_;

  FieldDistinctBuckets(String dataset, String field, long estimate) {
    this(dataset, field, Term.TYPE_NA, Sets.newHashSet(), estimate);
  }

  private FieldDistinctBuckets(String dataset, String field, int type, Set<String> labels,
      long estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = type;
    labels_ = new HashSet<>(labels);
    estimate_ = estimate;
  }

  public static Mutation newMutation(String dataset, String typedField, long estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(typedField, "typedField should not be null");
    Preconditions.checkState(
        typedField.indexOf(SEPARATOR_NUL) == typedField.lastIndexOf(SEPARATOR_NUL),
        "typedField format should be field\\0type");

    Text row = new Text(dataset + SEPARATOR_NUL + typedField);

    Text cf = new Text(DISTINCT_BUCKETS);

    ColumnVisibility cv = new ColumnVisibility(STRING_ADM + SEPARATOR_PIPE
        + AbstractStorage.toVisibilityLabel(dataset + SEPARATOR_UNDERSCORE + DISTINCT_BUCKETS));

    Value value = new Value(Long.toString(estimate, 10));

    Mutation mutation = new Mutation(row);
    mutation.put(cf, TEXT_EMPTY, cv, value);

    return mutation;
  }

  public static FieldDistinctBuckets fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract dataset, term and term's type from ROW
    @Var
    int index = row.indexOf(SEPARATOR_NUL);
    String dataset = row.substring(0, index);
    String typedField = row.substring(index + 1);

    index = typedField.indexOf(SEPARATOR_NUL);
    String field = typedField.substring(0, index);
    int type = Integer.parseInt(typedField.substring(index + 1), 10);

    // Extract visibility labels from CV
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract estimate from VALUE
    long estimate = Long.parseLong(val, 10);
    return new FieldDistinctBuckets(dataset, field, type, labels, estimate);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("field", field_)
        .add("type", type_).add("labels", labels_).add("estimate", estimate_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof FieldDistinctBuckets)) {
      return false;
    }
    FieldDistinctBuckets term = (FieldDistinctBuckets) obj;
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(field_, term.field_)
        && Objects.equal(type_, term.type_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(estimate_, term.estimate_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, field_, type_, labels_, estimate_);
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
  public int type() {
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
  public boolean isNa() {
    return type_ == TYPE_NA;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public long estimate() {
    return estimate_;
  }
}
