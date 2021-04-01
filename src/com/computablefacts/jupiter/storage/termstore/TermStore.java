package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.combiners.TermStoreCombiner;
import com.computablefacts.jupiter.filters.TermStoreDocFieldFilter;
import com.computablefacts.jupiter.filters.TermStoreFieldFilter;
import com.computablefacts.jupiter.filters.WildcardFilter;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.BigDecimalCodec;
import com.computablefacts.nona.helpers.Strings;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * This storage layer utilizes the <a href="https://accumulo.apache.org">Accumulo</a> table schemas
 * described below as the basis for its ingest and query components.
 * </p>
 *
 * <pre>
 *  Row                     | Column Family   | Column Qualifier               | Visibility                               | Value
 * =========================+=================+================================+==========================================+=================================
 *  <field>\0<term_type>    | <dataset>_CNT   | (empty)                        | ADM|<dataset>_CNT                        | <#occurrences>
 *  <field>\0<term_type>    | <dataset>_VIZ   | (empty)                        | ADM|<dataset>_VIZ                        | viz1\0viz2\0
 *  <mret>                  | <dataset>_BCNT  | <field>\0<term_type>           | ADM|<dataset>_<field>                    | <#occurrences>
 *  <mret>                  | <dataset>_BIDX  | <doc_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<doc_id> | <#occurrences>\0begin1\0end1...
 *  <term>                  | <dataset>_FCNT  | <field>\0<term_type>           | ADM|<dataset>_<field>                    | <#occurrences>
 *  <term>                  | <dataset>_FIDX  | <doc_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<doc_id> | <#occurrences>\0begin1\0end1...
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class TermStore extends AbstractStorage {

  private static final Logger logger_ = LoggerFactory.getLogger(TermStore.class);

  public TermStore(Configurations configurations, String name) {
    super(configurations, name);
  }

  static String count(String dataset) {
    return dataset + "_CNT";
  }

  static String visibility(String dataset) {
    return dataset + "_VIZ";
  }

  private static String forwardCount(String dataset) {
    return dataset + "_FCNT";
  }

  private static String forwardIndex(String dataset) {
    return dataset + "_FIDX";
  }

  private static String backwardCount(String dataset) {
    return dataset + "_BCNT";
  }

  private static String backwardIndex(String dataset) {
    return dataset + "_BIDX";
  }

  private static Iterator<Term> scanTerms(ScannerBase scanner, String dataset,
      Set<String> keepFields, BloomFilters<String> keepDocs, boolean isTermBackward, Range range) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    @Var
    boolean add = false;
    IteratorSetting setting =
        new IteratorSetting(22, "TermStoreDocFieldFilter", TermStoreDocFieldFilter.class);

    if (keepFields != null && !keepFields.isEmpty()) {
      add = true;
      TermStoreDocFieldFilter.setFieldsToKeep(setting, keepFields);
    }
    if (keepDocs != null) {
      add = true;
      TermStoreDocFieldFilter.setDocsToKeep(setting, keepDocs);
    }
    if (add) {
      scanner.addScanIterator(setting);
    }

    if (dataset != null) {
      scanner.fetchColumnFamily(new Text(dataset));
    } else {

      IteratorSetting settings = new IteratorSetting(23, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnColumnFamily(settings);
      WildcardFilter.addWildcard(settings, isTermBackward ? "*_BIDX" : "*_FIDX");

      scanner.addScanIterator(settings);
    }
    if (!setRange(scanner, range)) {
      return Constants.ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String termm = isTermBackward ? reverse(key.getRow().toString()) : key.getRow().toString();

      // Extract document id and field from CQ
      String cq = key.getColumnQualifier().toString();
      int index = cq.indexOf(Constants.SEPARATOR_NUL);
      String docId = cq.substring(0, index);
      int index2 = cq.lastIndexOf(Constants.SEPARATOR_NUL);

      String field;
      int termType;

      if (index == index2) {
        field = cq.substring(index + 1);
        termType = Term.TYPE_UNKNOWN;
      } else {
        field = cq.substring(index + 1, index2);
        termType = Integer.parseInt(cq.substring(index2 + 1), 10);
      }

      // Extract count and spans from VALUE
      String val = value.toString();
      List<String> spans = Splitter.on(Constants.SEPARATOR_NUL).splitToList(val);
      long count = Long.parseLong(spans.get(0), 10);
      List<org.apache.accumulo.core.util.ComparablePair<Integer, Integer>> ranges =
          new ArrayList<>((spans.size() - 1) / 2);

      for (int i = 2; i < spans.size(); i += 2) { // skip the count at position 0

        int begin = Integer.parseInt(spans.get(i - 1));
        int end = Integer.parseInt(spans.get(i));

        ranges.add(new org.apache.accumulo.core.util.ComparablePair<>(begin, end));
      }

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new Term(docId, field, termType, termm, labels, count, ranges);
    });
  }

  private static Iterator<TermCount> scanCounts(ScannerBase scanner, String dataset,
      Set<String> keepFields, boolean isTermBackward, Range range) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    if (keepFields != null && !keepFields.isEmpty()) {
      IteratorSetting setting =
          new IteratorSetting(22, "TermStoreFieldFilter", TermStoreFieldFilter.class);
      TermStoreFieldFilter.setFieldsToKeep(setting, keepFields);
      scanner.addScanIterator(setting);
    }

    if (dataset != null) {
      scanner.fetchColumnFamily(new Text(dataset));
    } else {

      IteratorSetting setting = new IteratorSetting(23, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnColumnFamily(setting);
      WildcardFilter.addWildcard(setting, isTermBackward ? "*_BCNT" : "*_FCNT");

      scanner.addScanIterator(setting);
    }
    if (!setRange(scanner, range)) {
      return Constants.ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String termm = isTermBackward ? reverse(key.getRow().toString()) : key.getRow().toString();

      // Extract field from CQ
      String cq = key.getColumnQualifier().toString();
      int index = cq.indexOf(Constants.SEPARATOR_NUL);

      String field;
      int termType;

      if (index < 0) {
        field = cq;
        termType = Term.TYPE_UNKNOWN;
      } else {
        field = cq.substring(0, index);
        termType = Integer.parseInt(cq.substring(index + 1), 10);
      }

      // Extract count from VALUE
      long count = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new TermCount(field, termType, termm, labels, count);
    });
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  @Override
  public boolean create() {

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
    }

    if (!isReady()) {
      if (super.create()) {
        try {

          // Set combiner
          IteratorSetting setting = new IteratorSetting(7, TermStoreCombiner.class);
          TermStoreCombiner.setCombineAllColumns(setting, true);
          TermStoreCombiner.setReduceOnFullCompactionOnly(setting, true);

          configurations().tableOperations().attachIterator(tableName(), setting);

          // Set default splits on [a-z0-9]
          SortedSet<Text> splits = new TreeSet<>();

          for (char i = '0'; i < '9' + 1; i++) {
            splits.add(new Text(Character.toString(i)));
          }

          for (char i = 'a'; i < 'z' + 1; i++) {
            splits.add(new Text(Character.toString(i)));
          }

          configurations().tableOperations().addSplits(tableName(), splits);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
          logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Remove all data from the table. Existing splits are kept.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  @Override
  public boolean truncate() {

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
    }

    if (!super.truncate()) {
      return false;
    }
    try {

      // Set default splits on [a-z0-9]
      SortedSet<Text> splits = new TreeSet<>();

      for (char i = '0'; i < '9' + 1; i++) {
        splits.add(new Text(Character.toString(i)));
      }

      for (char i = 'a'; i < 'z' + 1; i++) {
        splits.add(new Text(Character.toString(i)));
      }

      configurations().tableOperations().addSplits(tableName(), splits);
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
      return false;
    }
    return true;
  }

  /**
   * Remove all data for a given dataset.
   *
   * @param deleter batch deleter.
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeDataset(BatchDeleter deleter, String dataset) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).formatInfo());
    }

    Set<String> cfs = Sets.newHashSet(count(dataset), visibility(dataset), forwardCount(dataset),
        forwardIndex(dataset), backwardCount(dataset), backwardIndex(dataset));
    return remove(deleter, cfs);
  }

  /**
   * Remove documents from a given dataset. This method does not update the *CARD et *CNT datasets.
   * Hence, cardinalities and counts may become out of sync.
   *
   * @param deleter batch deleter.
   * @param dataset dataset.
   * @param docIds a set of documents ids to remove.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeDocuments(BatchDeleter deleter, String dataset, Set<String> docIds) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docIds, "docIds should not be null");
    Preconditions.checkArgument(!docIds.isEmpty(), "docIds should contain at least one id");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("doc_ids", docIds).formatInfo());
    }

    IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
    WildcardFilter.applyOnColumnQualifier(setting);
    WildcardFilter.setOr(setting);

    for (String docId : docIds) {
      WildcardFilter.addWildcard(setting, docId + Constants.SEPARATOR_NUL + "*");
    }

    try {
      deleter.addScanIterator(setting);
      deleter.setRanges(Collections.singleton(new Range()));
      deleter.fetchColumnFamily(new Text(forwardIndex(dataset)));
      deleter.fetchColumnFamily(new Text(backwardIndex(dataset)));
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
      return false;
    }
    return true;
  }

  /**
   * Remove term from a given dataset.
   *
   * @param deleter batch deleter.
   * @param dataset dataset.
   * @param term term.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeTerm(BatchDeleter deleter, String dataset, String term) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("term", term).formatInfo());
    }

    @Var
    boolean isOk = true;

    isOk = isOk && super.remove(deleter, term, forwardIndex(dataset), null);
    isOk = isOk && super.remove(deleter, term, forwardCount(dataset), null);

    isOk = isOk && super.remove(deleter, reverse(term), backwardIndex(dataset), null);
    isOk = isOk && super.remove(deleter, reverse(term), backwardCount(dataset), null);

    return isOk;
  }

  /**
   * Group data belonging to a same dataset together.
   *
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean addLocalityGroup(String dataset) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("add_locality_group", tableName())
          .add("dataset", dataset).formatInfo());
    }

    Map<String, Set<Text>> groups =
        Tables.getLocalityGroups(configurations().tableOperations(), tableName());

    int size = groups.size();

    String forwardCount = forwardCount(dataset);
    String forwardIndex = forwardIndex(dataset);
    String backwardCount = backwardCount(dataset);
    String backwardIndex = backwardIndex(dataset);

    if (!groups.containsKey(forwardCount)) {
      groups.put(forwardCount, Sets.newHashSet(new Text(forwardCount)));
    }
    if (!groups.containsKey(forwardIndex)) {
      groups.put(forwardIndex, Sets.newHashSet(new Text(forwardIndex)));
    }
    if (!groups.containsKey(backwardCount)) {
      groups.put(backwardCount, Sets.newHashSet(new Text(backwardCount)));
    }
    if (!groups.containsKey(backwardIndex)) {
      groups.put(backwardIndex, Sets.newHashSet(new Text(backwardIndex)));
    }
    return size == groups.size()
        || Tables.setLocalityGroups(configurations().tableOperations(), tableName(), groups, false);
  }

  /**
   * Persist data. Term extraction for a given field from a given document should be performed by
   * the caller. This method should be called only once for each (dataset, docId, field, term).
   *
   * @param writer batch writer.
   * @param dataset dataset.
   * @param docId document id.
   * @param field field name.
   * @param termType the type of the term i.e. string, number, etc.
   * @param term term.
   * @param spans positions of the term in the document.
   * @param docSpecificLabels visibility labels specific to a given document.
   * @param fieldSpecificLabels visibility labels specific to a given field.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean add(BatchWriter writer, String dataset, String docId, String field, int termType,
      String term, List<Pair<Integer, Integer>> spans, Set<String> docSpecificLabels,
      Set<String> fieldSpecificLabels) {
    return add(writer, dataset, docId, field, termType, term, spans, docSpecificLabels,
        fieldSpecificLabels, false);
  }

  /**
   * Persist data. Term extraction for a given field from a given document should be performed by
   * the caller. This method should be called only once for each (dataset, docId, field, term).
   *
   * @param writer batch writer.
   * @param dataset dataset.
   * @param docId document id.
   * @param field field name.
   * @param termType the type of the term i.e. string, number, etc.
   * @param term term.
   * @param spans positions of the term in the document.
   * @param docSpecificLabels visibility labels specific to a given document.
   * @param fieldSpecificLabels visibility labels specific to a given field.
   * @param writeInForwardIndexOnly allow the caller to explicitly specify that the term must be
   *        written in the forward index only.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean add(BatchWriter writer, String dataset, String docId, String field, int termType,
      String term, List<Pair<Integer, Integer>> spans, Set<String> docSpecificLabels,
      Set<String> fieldSpecificLabels, boolean writeInForwardIndexOnly) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(spans, "spans should not be null");
    Preconditions.checkNotNull(docSpecificLabels, "docSpecificLabels should not be null");
    Preconditions.checkNotNull(fieldSpecificLabels, "fieldSpecificLabels should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.info(
          LogFormatterManager.logFormatter().add("table_name", tableName()).add("dataset", dataset)
              .add("doc_id", docId).add("field", field).add("term_type", termType).add("term", term)
              .add("doc_specific_labels", docSpecificLabels)
              .add("field_specific_labels", fieldSpecificLabels).formatDebug());
    }

    Text newTerm = new Text(term);
    Text newTermReversed = new Text(reverse(term));
    Text newField = new Text(field + Constants.SEPARATOR_NUL + termType);
    Text newDocField =
        new Text(docId + Constants.SEPARATOR_NUL + field + Constants.SEPARATOR_NUL + termType);
    Value newSpans =
        new Value(
            Integer.toString(spans.size(), 10) + Constants.SEPARATOR_NUL
                + Joiner.on(Constants.SEPARATOR_NUL)
                    .join(spans
                        .stream().map(p -> Integer.toString(p.getFirst(), 10)
                            + Constants.SEPARATOR_NUL + Integer.toString(p.getSecond(), 10))
                        .collect(Collectors.toList())));
    Value newCount = new Value(Integer.toString(spans.size(), 10));

    // Column visibility from labels
    ColumnVisibility vizFieldCount = new ColumnVisibility(Constants.STRING_ADM
        + Constants.SEPARATOR_PIPE + AbstractStorage.toVisibilityLabel(count(dataset)));
    ColumnVisibility vizFieldLabels = new ColumnVisibility(Constants.STRING_ADM
        + Constants.SEPARATOR_PIPE + AbstractStorage.toVisibilityLabel(visibility(dataset)));

    ColumnVisibility vizFieldSpecific =
        new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE).join(fieldSpecificLabels));
    ColumnVisibility viz = new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE)
        .join(Sets.union(docSpecificLabels, fieldSpecificLabels)));

    // Ingest stats
    @Var
    boolean isOk = add(writer, newField, new Text(count(dataset)), null, vizFieldCount, newCount);
    isOk = isOk && add(writer, newField, new Text(visibility(dataset)), null, vizFieldLabels,
        new Value(Joiner.on(Constants.SEPARATOR_NUL).join(fieldSpecificLabels)));

    // Forward index
    isOk = isOk && add(writer, newTerm, new Text(forwardCount(dataset)), newField, vizFieldSpecific,
        newCount);
    isOk =
        isOk && add(writer, newTerm, new Text(forwardIndex(dataset)), newDocField, viz, newSpans);

    if (!writeInForwardIndexOnly) {

      // Backward index
      isOk = isOk && add(writer, newTermReversed, new Text(backwardCount(dataset)), newField,
          vizFieldSpecific, newCount);
      isOk = isOk && add(writer, newTermReversed, new Text(backwardIndex(dataset)), newDocField,
          viz, newSpans);
    }
    return isOk;
  }

  /**
   * Get the number of terms indexed for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields.
   * @return count.
   */
  public Iterator<FieldCount> fieldCount(ScannerBase scanner, String dataset, Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(count(dataset)));

    if (fields != null) {

      List<Range> ranges = fields.stream().map(field -> field + Constants.SEPARATOR_NUL)
          .map(Range::prefix).collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return Constants.ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String cq = key.getRow().toString();
      int index = cq.indexOf(Constants.SEPARATOR_NUL);

      String field;
      int termType;

      if (index < 0) {
        field = cq;
        termType = Term.TYPE_UNKNOWN;
      } else {
        field = cq.substring(0, index);
        termType = Integer.parseInt(cq.substring(index + 1), 10);
      }

      // Extract term count from VALUE
      long count = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new FieldCount(field, termType, labels, count);
    });
  }

  /**
   * Get the visibility labels associated to each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields.
   * @return visibility labels.
   */
  public Iterator<FieldLabels> fieldLabels(ScannerBase scanner, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(visibility(dataset)));

    if (fields != null) {

      List<Range> ranges = fields.stream().map(field -> field + Constants.SEPARATOR_NUL)
          .map(Range::prefix).collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return Constants.ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String row = key.getRow().toString();
      int index = row.indexOf(Constants.SEPARATOR_NUL);

      String field;
      int termType;

      if (index < 0) {
        field = row;
        termType = Term.TYPE_UNKNOWN;
      } else {
        field = row.substring(0, index);
        termType = Integer.parseInt(row.substring(index + 1), 10);
      }

      // Extract term labels from VALUE
      Set<String> labelsTerm =
          Sets.newHashSet(Splitter.on(Constants.SEPARATOR_NUL).split(value.toString()));

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labelsAccumulo = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new FieldLabels(field, termType, labelsAccumulo, labelsTerm);
    });
  }

  /**
   * Get the number of occurrences of all numbers in a given range for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param minTerm number (optional). Beginning of the range (included).
   * @param maxTerm number (optional). End of the range (excluded).
   * @param keepFields fields patterns to keep (optional).
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCount> numericalRangeCount(ScannerBase scanner, String dataset,
      String minTerm, String maxTerm, Set<String> keepFields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");

    if (minTerm != null && !Strings.isNumber(minTerm)) {
      logger_.error(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).message("minTerm must be a number!")
          .formatError());
      return Constants.ITERATOR_EMPTY;
    }
    if (maxTerm != null && !Strings.isNumber(maxTerm)) {
      logger_.error(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).message("maxTerm must be a number!")
          .formatError());
      return Constants.ITERATOR_EMPTY;
    }

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    Range range;

    if (minTerm == null) {
      Key startKey = new Key();
      Key endKey = new Key(BigDecimalCodec.encode(maxTerm));
      range = new Range(startKey, endKey);
    } else if (maxTerm == null) {
      Key startKey = new Key(BigDecimalCodec.encode(minTerm));
      range = new Range(startKey, null);
    } else {
      Key startKey = new Key(BigDecimalCodec.encode(minTerm));
      Key endKey = new Key(BigDecimalCodec.encode(maxTerm));
      range = new Range(startKey, endKey);
    }

    String newDataset = dataset == null ? null : forwardCount(dataset);

    return Iterators.transform(
        Iterators.filter(scanCounts(scanner, newDataset, keepFields, false, range),
            TermCount::isNumber),
        term -> new TermCount(term.field(), term.termType(), BigDecimalCodec.decode(term.term()),
            term.labels(), term.count()));
  }

  /**
   * Get the number of occurrences of each term for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCount> termCount(ScannerBase scanner, String dataset, String term) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("term", term).formatInfo());
    }

    // TODO: add "filter by field"

    scanner.clearColumns();
    scanner.clearScanIterators();

    String newTerm;
    String newDataset;

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);

    if (isTermBackward) {
      newTerm = reverse(term);
      newDataset = dataset == null ? null : backwardCount(dataset);
    } else {
      newTerm = term;
      newDataset = dataset == null ? null : forwardCount(dataset);
    }

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      if (newDataset == null) {
        range = Range.exact(newTerm);
      } else {
        range = Range.exact(newTerm, newDataset);
      }
    } else {

      range = Range.prefix(WildcardMatcher.prefix(newTerm));

      IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, newTerm);

      scanner.addScanIterator(setting);
    }
    return scanCounts(scanner, newDataset, Sets.newHashSet(), isTermBackward, range);
  }

  /**
   * Get documents, fields and spans. This method only works for scanning numerical ranges.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param minTerm number (optional). Beginning of the range (included).
   * @param maxTerm number (optional). End of the range (excluded).
   * @param keepFields fields patterns to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> numericalRangeScan(Scanner scanner, String dataset, String minTerm,
      String maxTerm, Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");

    if (minTerm != null && !Strings.isNumber(minTerm)) {
      logger_.error(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).add("has_keep_docs", keepDocs != null)
          .message("minTerm must be a number!").formatError());
      return Constants.ITERATOR_EMPTY;
    }
    if (maxTerm != null && !Strings.isNumber(maxTerm)) {
      logger_.error(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).add("has_keep_docs", keepDocs != null)
          .message("maxTerm must be a number!").formatError());
      return Constants.ITERATOR_EMPTY;
    }

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("minTerm", minTerm).add("maxTerm", maxTerm)
          .add("has_keep_fields", keepFields != null).add("has_keep_docs", keepDocs != null)
          .formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    Range range;

    if (minTerm == null) {
      Key startKey = new Key();
      Key endKey = new Key(BigDecimalCodec.encode(maxTerm));
      range = new Range(startKey, endKey);
    } else if (maxTerm == null) {
      Key startKey = new Key(BigDecimalCodec.encode(minTerm));
      range = new Range(startKey, null);
    } else {
      Key startKey = new Key(BigDecimalCodec.encode(minTerm));
      Key endKey = new Key(BigDecimalCodec.encode(maxTerm));
      range = new Range(startKey, endKey);
    }

    String newDataset = dataset == null ? null : forwardIndex(dataset);

    return Iterators.transform(
        Iterators.filter(scanTerms(scanner, newDataset, keepFields, keepDocs, false, range),
            Term::isNumber),
        term -> new Term(term.docId(), term.field(), term.termType(),
            BigDecimalCodec.decode(term.term()), term.labels(), term.count(), term.spans()));
  }

  /**
   * Get documents, fields and spans.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term term. Might contain wildcard characters.
   * @param keepFields fields patterns to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> termScan(ScannerBase scanner, String dataset, String term,
      Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("term", term).add("has_keep_fields", keepFields != null)
          .add("has_keep_docs", keepDocs != null).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    String newTerm;
    String newDataset;

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);

    if (isTermBackward) {
      newTerm = reverse(term);
      newDataset = dataset == null ? null : backwardIndex(dataset);
    } else {
      newTerm = term;
      newDataset = dataset == null ? null : forwardIndex(dataset);
    }

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      if (newDataset == null) {
        range = Range.exact(newTerm);
      } else {
        range = Range.exact(newTerm, newDataset);
      }
    } else {

      range = Range.prefix(WildcardMatcher.prefix(newTerm));

      IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, newTerm);

      scanner.addScanIterator(setting);
    }
    return scanTerms(scanner, newDataset, keepFields, keepDocs, isTermBackward, range);
  }
}
