package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.nona.Generated;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class TermDistinctBuckets implements HasTerm {

  private final String dataset_;
  private final String field_;
  private final int type_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;

  TermDistinctBuckets(String dataset, String field, int type, String term, Set<String> labels,
      long count) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    dataset_ = dataset;
    field_ = field;
    type_ = type;
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  public static Mutation newForwardMutation(String dataset, String field, int type, String term,
      int count, Set<String> labels) {
    return newForwardMutation(null, dataset, field, type, term, count, labels);
  }

  public static Mutation newBackwardMutation(String dataset, String field, int type, String term,
      int count, Set<String> labels) {
    return newBackwardMutation(null, dataset, field, type, term, count, labels);
  }

  @CanIgnoreReturnValue
  public static Mutation newForwardMutation(Map<Text, Mutation> mutations, String dataset,
      String field, int type, String term, int count, Set<String> labels) {
    return newMutation(mutations, TermStore.forwardCount(dataset), field, type, term, count,
        labels);
  }

  @CanIgnoreReturnValue
  public static Mutation newBackwardMutation(Map<Text, Mutation> mutations, String dataset,
      String field, int type, String term, int count, Set<String> labels) {
    return newMutation(mutations, TermStore.backwardCount(dataset), field, type, reverse(term),
        count, labels);
  }

  public static TermDistinctBuckets fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cq = key.getColumnQualifier().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract term from ROW
    String term = cf.endsWith("_BCNT") ? reverse(row) : row;

    // Extract dataset from CF
    String datazet = cf.substring(0, cf.lastIndexOf('_'));

    // Extract field from CQ
    int index = cq.indexOf(SEPARATOR_NUL);

    String field;
    int type;

    if (index < 0) {
      field = cq;
      type = Term.TYPE_UNKNOWN;
    } else {
      field = cq.substring(0, index);
      type = Integer.parseInt(cq.substring(index + 1), 10);
    }

    // Extract visibility labels from CV
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    return new TermDistinctBuckets(datazet, field, type, term, labels, Long.parseLong(val, 10));
  }

  private static Mutation newMutation(Map<Text, Mutation> mutations, String dataset, String field,
      int type, String term, int count, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    Text row = new Text(term);

    Text cq = new Text(field + SEPARATOR_NUL + type);

    ColumnVisibility cv = new ColumnVisibility(Joiner.on(SEPARATOR_PIPE).join(labels));

    Value value = new Value(Integer.toString(count, 10));

    Mutation mutation;

    if (mutations == null || !mutations.containsKey(row)) {

      mutation = new Mutation(row);

      if (mutations != null) {
        mutations.put(row, mutation);
      }
    } else {
      mutation = mutations.get(row);
    }

    mutation.put(new Text(dataset), cq, cv, value);

    return mutation;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("field", field_)
        .add("type", type_).add("term", term_).add("labels", labels_).add("count", count_)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TermDistinctBuckets)) {
      return false;
    }
    TermDistinctBuckets term = (TermDistinctBuckets) obj;
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(field_, term.field_)
        && Objects.equal(term_, term.term_) && Objects.equal(type_, term.type_)
        && Objects.equal(labels_, term.labels_) && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, field_, type_, term_, labels_, count_);
  }

  @Generated
  @Override
  public String term() {
    return term_;
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
  public long count() {
    return count_;
  }
}
