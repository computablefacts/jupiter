package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FieldLastUpdate {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final Set<String> labels_;
  private final String lastUpdate_;

  FieldLastUpdate(String dataset, String field, int termType, Set<String> labels,
      String lastUpdate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = termType;
    labels_ = new HashSet<>(labels);
    lastUpdate_ = lastUpdate;
  }

  public static Mutation newMutation(String dataset, String field, int type) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    Text row = new Text(field + SEPARATOR_NUL + type);

    Text cf = new Text(TermStore.lastUpdate(dataset));

    ColumnVisibility cv = new ColumnVisibility(STRING_ADM + SEPARATOR_PIPE
        + AbstractStorage.toVisibilityLabel(TermStore.lastUpdate(dataset)));

    Value value = new Value(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

    Mutation mutation = new Mutation(row);
    mutation.put(cf, TEXT_EMPTY, cv, value);

    return mutation;
  }

  public static FieldLastUpdate fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString(); // last update

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

    return new FieldLastUpdate(datazet, field, type, labels, val);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("field", field_)
        .add("type", type_).add("labels", labels_).add("last_update", lastUpdate_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof FieldLastUpdate)) {
      return false;
    }
    FieldLastUpdate term = (FieldLastUpdate) obj;
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(field_, term.field_)
        && Objects.equal(type_, term.type_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(lastUpdate_, term.lastUpdate_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, field_, type_, labels_, lastUpdate_);
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
  public String lastUpdate() {
    return lastUpdate_;
  }
}
