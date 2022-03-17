package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.*;
import static com.computablefacts.jupiter.storage.termstore.TermStore.VISIBILITY;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.asterix.Generated;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.*;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class FieldLabels {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final Set<String> labelsTerm_;

  FieldLabels(String dataset, String field, int type, Set<String> labels) {
    this(dataset, field, type, labels, Sets.newHashSet(STRING_ADM,
        AbstractStorage.toVisibilityLabel(dataset + SEPARATOR_UNDERSCORE + VISIBILITY)));
  }

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
    return newMutation(null, dataset, field, type, labels);
  }

  @CanIgnoreReturnValue
  public static Mutation newMutation(Map<Text, Mutation> mutations, String dataset, String field,
      int type, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    Text row = new Text(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL + type);

    Text cf = new Text(VISIBILITY);

    ColumnVisibility cv = new ColumnVisibility(STRING_ADM + SEPARATOR_PIPE
        + AbstractStorage.toVisibilityLabel(dataset + SEPARATOR_UNDERSCORE + VISIBILITY));

    Value value = new Value(Joiner.on(Constants.SEPARATOR_NUL).join(labels));

    Mutation mutation;

    if (mutations == null || !mutations.containsKey(row)) {

      mutation = new Mutation(row);

      if (mutations != null) {
        mutations.put(row, mutation);
      }
    } else {
      mutation = mutations.get(row);
    }

    mutation.put(cf, TEXT_EMPTY, cv, value);

    return mutation;
  }

  public static FieldLabels fromKeyValue(Key key, Value value) {

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

    // Extract term labels from VALUE
    Set<String> labelsTerm = Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(val));
    return new FieldLabels(dataset, field, type, labels, labelsTerm);
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
  public boolean isNa() {
    return type_ == Term.TYPE_NA;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public Set<String> termLabels() {
    return labelsTerm_;
  }

  @Generated
  void update(Collection<String> newLabels) {
    labelsTerm_.addAll(newLabels);
  }
}
