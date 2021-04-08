package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ComparablePair;

import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.BigDecimalCodec;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Term implements HasDataset, HasTerm, HasTermType, Comparable<Term> {

  public static final int TYPE_UNKNOWN = 0;
  public static final int TYPE_STRING = 1;
  public static final int TYPE_NUMBER = 2;
  public static final int TYPE_DATE = 3;
  public static final int TYPE_BOOLEAN = 4;

  private final String dataset_;
  private final String docId_;
  private final String field_;
  private final int type_;
  private final String term_;
  private final Set<String> labels_;
  private final long count_;
  private final List<ComparablePair<Integer, Integer>> spans_;

  Term(String dataset, String docId, String field, int type, String term, Set<String> labels,
      long count, List<ComparablePair<Integer, Integer>> spans) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(spans, "spans should not be null");

    Preconditions.checkState(count == spans.size(),
        "inconsistent state for (field, term, count, spans) : (%s, %s, %s, %s)", field, term, count,
        spans);

    dataset_ = dataset;
    docId_ = docId;
    field_ = field;
    type_ = type;
    term_ = term;
    labels_ = new HashSet<>(labels);
    count_ = count;
    spans_ = new ArrayList<>(spans);
  }

  public static Term fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString();
    String cf = key.getColumnFamily().toString();
    String cq = key.getColumnQualifier().toString();
    String cv = key.getColumnVisibility().toString();
    String val = value.toString();

    // Extract term from ROW
    String term = cf.endsWith("_BIDX") ? reverse(row) : row;

    // Extract dataset from CF
    String datazet = cf.substring(0, cf.lastIndexOf('_'));

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
    List<org.apache.accumulo.core.util.ComparablePair<Integer, Integer>> ranges =
        new ArrayList<>((spans.size() - 1) / 2);

    for (int i = 2; i < spans.size(); i += 2) { // skip the count at position 0

      int begin = Integer.parseInt(spans.get(i - 1));
      int end = Integer.parseInt(spans.get(i));

      ranges.add(new org.apache.accumulo.core.util.ComparablePair<>(begin, end));
    }
    return new Term(datazet, docId, field, type,
        type == Term.TYPE_NUMBER ? BigDecimalCodec.decode(term) : term, labels, count, ranges);
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("docId", docId_)
        .add("field", field_).add("term", term_).add("type", type_).add("labels", labels_)
        .add("count", count_).add("spans", spans_).toString();
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
    return Objects.equal(dataset_, term.dataset_) && Objects.equal(docId_, term.docId_)
        && Objects.equal(field_, term.field_) && Objects.equal(type_, term.type_)
        && Objects.equal(term_, term.term_) && Objects.equal(labels_, term.labels_)
        && Objects.equal(count_, term.count_) && Objects.equal(spans_, term.spans_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, docId_, field_, type_, term_, labels_, count_, spans_);
  }

  @Override
  public int compareTo(@NotNull Term term) {

    @Var
    int cmp = ComparisonChain.start().compare(dataset_, term.dataset_).compare(docId_, term.docId_)
        .compare(field_, term.field_).compare(type_, term.type_).compare(term_, term.term_)
        .compare(count_, term.count_).result();

    if (cmp != 0) {
      return cmp;
    }

    cmp = compare(labels_, term.labels_);

    if (cmp != 0) {
      return cmp;
    }
    return compare(spans_, term.spans_);
  }

  @Generated
  @Override
  public String dataset() {
    return dataset_;
  }

  @Generated
  @Override
  public int termType() {
    return type_;
  }

  @Generated
  @Override
  public String term() {
    return term_;
  }

  @Generated
  @Override
  public boolean isUnknown() {
    return type_ == TYPE_UNKNOWN;
  }

  @Generated
  @Override
  public boolean isString() {
    return type_ == TYPE_STRING;
  }

  @Generated
  @Override
  public boolean isNumber() {
    return type_ == TYPE_NUMBER;
  }

  @Generated
  @Override
  public boolean isDate() {
    return type_ == TYPE_DATE;
  }

  @Generated
  public String docId() {
    return docId_;
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

  @Generated
  public List<ComparablePair<Integer, Integer>> spans() {
    return spans_;
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
