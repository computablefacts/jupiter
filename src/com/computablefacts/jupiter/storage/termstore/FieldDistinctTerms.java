package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_UNDERSCORE;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;
import static com.computablefacts.jupiter.storage.termstore.TermStore.DISTINCT_TERMS;

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
final public class FieldDistinctTerms {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final double estimate_;

  FieldDistinctTerms(String dataset, String field, int type, Set<String> labels, byte[] sketch) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(sketch, "sketch should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = type;
    labels_ = new HashSet<>(labels);
    estimate_ = ThetaSketch.wrap(sketch).getEstimate();
  }

  public static Mutation newMutation(String dataset, String typedField, byte[] sketch) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(typedField, "typedField should not be null");
    Preconditions.checkState(
        typedField.indexOf(SEPARATOR_NUL) == typedField.lastIndexOf(SEPARATOR_NUL),
        "typedField format should be typed\\0type");
    Preconditions.checkNotNull(sketch, "sketch should not be null");

    Text row = new Text(dataset + SEPARATOR_NUL + typedField);

    Text cf = new Text(DISTINCT_TERMS);

    ColumnVisibility cv = new ColumnVisibility(STRING_ADM + SEPARATOR_PIPE
        + AbstractStorage.toVisibilityLabel(dataset + SEPARATOR_UNDERSCORE + DISTINCT_TERMS));

    Value value = new Value(sketch);

    Mutation mutation = new Mutation(row);
    mutation.put(cf, TEXT_EMPTY, cv, value);

    return mutation;
  }

  public static FieldDistinctTerms fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cv = key.getColumnVisibility().toString();

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

    // Extract sketch from VALUE
    byte[] sketch = value.get();
    return new FieldDistinctTerms(dataset, field, type, labels, sketch);
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
    if (!(obj instanceof FieldDistinctTerms)) {
      return false;
    }
    FieldDistinctTerms term = (FieldDistinctTerms) obj;
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
    return type_ == Term.TYPE_NA;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public double estimate() {
    return estimate_;
  }
}
