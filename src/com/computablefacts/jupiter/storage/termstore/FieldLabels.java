package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.Generated;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldLabels {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final Set<String> labelsTerm_;

  FieldLabels(String dataset, String field, int type, Set<String> labels, Set<String> labelsTerm) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(labelsTerm, "labelsTerm should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = type;
    labels_ = new HashSet<>(labels);
    labelsTerm_ = new HashSet<>(labelsTerm);
  }

  public static Mutation newMutation(String dataset, String field, int type, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    Text row = new Text(field + SEPARATOR_NUL + type);

    Text cf = new Text(TermStore.visibility(dataset));

    ColumnVisibility cv = new ColumnVisibility(STRING_ADM + SEPARATOR_PIPE
        + AbstractStorage.toVisibilityLabel(TermStore.visibility(dataset)));

    Value value = new Value(Joiner.on(Constants.SEPARATOR_NUL).join(labels));

    Mutation mutation = new Mutation(row);
    mutation.put(cf, TEXT_EMPTY, cv, value);

    return mutation;
  }

  public static FieldLabels fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract term and term's type from ROW
    int index = row.indexOf(SEPARATOR_NUL);

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
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract term labels from VALUE
    Set<String> labelsTerm = Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(val));

    return new FieldLabels(datazet, field, type, labels, labelsTerm);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("field", field_)
        .add("type", type_).add("labels", labels_).add("labelsTerm", labelsTerm_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof FieldLabels)) {
      return false;
    }
    FieldLabels term = (FieldLabels) obj;
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(field_, term.field_)
        && Objects.equal(type_, term.type_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(labelsTerm_, term.labelsTerm_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, field_, type_, labels_, labelsTerm_);
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
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public Set<String> termLabels() {
    return labelsTerm_;
  }
}
