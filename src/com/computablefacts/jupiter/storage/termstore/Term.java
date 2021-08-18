package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Term implements HasTerm, Comparable<Term> {

  public static final int TYPE_UNKNOWN = 0;
  public static final int TYPE_STRING = 1;
  public static final int TYPE_NUMBER = 2;
  public static final int TYPE_DATE = 3;
  public static final int TYPE_BOOLEAN = 4;
  public static final int TYPE_NA = 5; // Not Applicable

  private final String dataset_;
  private final String bucketId_;
  private final String field_;
  private final int type_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;

  Term(String dataset, String bucketId, String field, int type, String term, Set<String> labels,
      long count) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(bucketId, "bucketId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    dataset_ = dataset;
    bucketId_ = bucketId;
    field_ = field;
    type_ = type;
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
  }

  public static Mutation newForwardMutation(String dataset, String docId, String field, int type,
      String term, int count, Set<String> labels) {
    return newForwardMutation(null, dataset, docId, field, type, term, count, labels);
  }

  public static Mutation newBackwardMutation(String dataset, String docId, String field, int type,
      String term, int count, Set<String> labels) {
    return newBackwardMutation(null, dataset, docId, field, type, term, count, labels);
  }

  @CanIgnoreReturnValue
  public static Mutation newForwardMutation(Map<Text, Mutation> mutations, String dataset,
      String docId, String field, int type, String term, int count, Set<String> labels) {
    return newMutation(mutations, TermStore.forwardIndex(), dataset, docId, field, type, term,
        count, labels);
  }

  @CanIgnoreReturnValue
  public static Mutation newBackwardMutation(Map<Text, Mutation> mutations, String dataset,
      String docId, String field, int type, String term, int count, Set<String> labels) {
    return newMutation(mutations, TermStore.backwardIndex(), dataset, docId, field, type,
        reverse(term), count, labels);
  }

  public static Term fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cq = key.getColumnQualifier().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract dataset and term from ROW
    int index = row.indexOf(SEPARATOR_NUL);
    String dataset = row.substring(0, index);
    String term = cf.equals(TermStore.backwardIndex()) ? reverse(row.substring(index + 1))
        : row.substring(index + 1);

    // Extract document id and field from CQ
    int index1 = cq.indexOf(SEPARATOR_NUL);
    String docId = cq.substring(0, index1);
    int index2 = cq.lastIndexOf(SEPARATOR_NUL);

    String field;
    int type;

    if (index1 == index2) {
      field = cq.substring(index1 + 1);
      type = Term.TYPE_UNKNOWN;
    } else {
      field = cq.substring(index1 + 1, index2);
      type = Integer.parseInt(cq.substring(index2 + 1), 10);
    }

    // Extract visibility labels from CV
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract count and spans from VALUE
    List<String> spans = Splitter.on(SEPARATOR_NUL).splitToList(val);
    long count = Long.parseLong(spans.get(0), 10);

    return new Term(dataset, docId, field, type, term, labels, count);
  }

  private static Mutation newMutation(Map<Text, Mutation> mutations, String mutationType,
      String dataset, String docId, String field, int type, String term, int count,
      Set<String> labels) {

    Preconditions.checkNotNull(mutationType, "mutationType should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(count > 0, "count must be > 0");
    Preconditions.checkNotNull(labels, "labels should not be null");

    Text row = new Text(dataset + SEPARATOR_NUL + term);

    Text cq = new Text(docId + SEPARATOR_NUL + field + SEPARATOR_NUL + type);

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

    mutation.put(new Text(mutationType), cq, cv, value);

    return mutation;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("docId", bucketId_)
        .add("field", field_).add("term", term_).add("type", type_).add("labels", labels_)
        .add("count", count_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Term)) {
      return false;
    }
    Term term = (Term) obj;
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(bucketId_, term.bucketId_)
        && Objects.equal(field_, term.field_) && Objects.equal(type_, term.type_)
        && Objects.equal(term_, term.term_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(count_, term.count_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, bucketId_, field_, type_, term_, labels_, count_);
  }

  @Override
  public int compareTo(@NotNull Term term) {

    @Var
    int cmp = ComparisonChain.start().compare(dataset_, term.dataset_)
        .compare(bucketId_, term.bucketId_).compare(field_, term.field_).compare(type_, term.type_)
        .compare(term_, term.term_).compare(count_, term.count_).result();

    if (cmp != 0) {
      return cmp;
    }
    return compare(labels_, term.labels_);
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
  public int type() {
    return type_;
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
  public boolean isNumber() {
    return type_ == TYPE_NUMBER;
  }

  @Generated
  public boolean isDate() {
    return type_ == TYPE_DATE;
  }

  @Generated
  public boolean isNa() {
    return type_ == Term.TYPE_NA;
  }

  @Generated
  public String bucketId() {
    return bucketId_;
  }

  @Generated
  public String field() {
    return field_;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public long count() {
    return count_;
  }

  private <T extends Comparable<T>> int compare(Collection<T> l1, Collection<T> l2) {

    @Var
    int cmp = Integer.compare(l1.size(), l2.size());

    if (cmp != 0) {
      return cmp;
    }

    List<T> tmp1 = Lists.newArrayList(l1);
    List<T> tmp2 = Lists.newArrayList(l2);

    Collections.sort(tmp1);
    Collections.sort(tmp2);

    for (int i = 0; i < tmp1.size(); i++) {

      cmp = tmp1.get(i).compareTo(tmp2.get(i));

      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }
}
