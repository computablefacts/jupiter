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
import com.computablefacts.jupiter.filters.WildcardFilter;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.annotations.Beta;
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
 *  Row        | Column Family   | Column Qualifier  | Visibility                               | Value
 * ============+=================+===================+==========================================+=================================
 *  <field>    | <dataset>_CARD  | (empty)           | ADM|<dataset>_CARD                       | <#documents>
 *  <field>    | <dataset>_CNT   | (empty)           | ADM|<dataset>_CNT                        | <#occurrences>
 *  <field>    | <dataset>_VIZ   | (empty)           | ADM|<dataset>_VIZ                        | viz1\0viz2\0
 *  <mret>     | <dataset>_BCARD | <field>           | ADM|<dataset>_<field>                    | <#documents>
 *  <mret>     | <dataset>_BCNT  | <field>           | ADM|<dataset>_<field>                    | <#occurrences>
 *  <mret>     | <dataset>_BIDX  | <doc_id>\0<field> | ADM|<dataset>_<field>|<dataset>_<doc_id> | <#occurrences>\0begin1\0end1...
 *  <term>     | <dataset>_FCARD | <field>           | ADM|<dataset>_<field>                    | <#documents>
 *  <term>     | <dataset>_FCNT  | <field>           | ADM|<dataset>_<field>                    | <#occurrences>
 *  <term>     | <dataset>_FIDX  | <doc_id>\0<field> | ADM|<dataset>_<field>|<dataset>_<doc_id> | <#occurrences>\0begin1\0end1...
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

  static String card(String dataset) {
    return dataset + "_CARD";
  }

  static String visibility(String dataset) {
    return dataset + "_VIZ";
  }

  private static String forwardCount(String dataset) {
    return dataset + "_FCNT";
  }

  private static String forwardCard(String dataset) {
    return dataset + "_FCARD";
  }

  private static String forwardIndex(String dataset) {
    return dataset + "_FIDX";
  }

  private static String backwardCount(String dataset) {
    return dataset + "_BCNT";
  }

  private static String backwardCard(String dataset) {
    return dataset + "_BCARD";
  }

  private static String backwardIndex(String dataset) {
    return dataset + "_BIDX";
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

    Set<String> cfs = Sets.newHashSet(count(dataset), card(dataset), visibility(dataset),
        forwardCount(dataset), forwardCard(dataset), forwardIndex(dataset), backwardCount(dataset),
        backwardCard(dataset), backwardIndex(dataset));
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
  @Beta
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
  @Beta
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
    isOk = isOk && super.remove(deleter, term, forwardCard(dataset), null);

    isOk = isOk && super.remove(deleter, reverse(term), backwardIndex(dataset), null);
    isOk = isOk && super.remove(deleter, reverse(term), backwardCount(dataset), null);
    isOk = isOk && super.remove(deleter, reverse(term), backwardCard(dataset), null);

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
    String forwardCard = forwardCard(dataset);
    String forwardIndex = forwardIndex(dataset);
    String backwardCount = backwardCount(dataset);
    String backwardCard = backwardCard(dataset);
    String backwardIndex = backwardIndex(dataset);

    if (!groups.containsKey(forwardCount)) {
      groups.put(forwardCount, Sets.newHashSet(new Text(forwardCount)));
    }
    if (!groups.containsKey(forwardCard)) {
      groups.put(forwardCard, Sets.newHashSet(new Text(forwardCard)));
    }
    if (!groups.containsKey(forwardIndex)) {
      groups.put(forwardIndex, Sets.newHashSet(new Text(forwardIndex)));
    }
    if (!groups.containsKey(backwardCount)) {
      groups.put(backwardCount, Sets.newHashSet(new Text(backwardCount)));
    }
    if (!groups.containsKey(backwardCard)) {
      groups.put(backwardCard, Sets.newHashSet(new Text(backwardCard)));
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
   * @param stats ingest stats.
   * @param dataset dataset.
   * @param docId document id.
   * @param field field name.
   * @param term term.
   * @param spans positions of the term in the document.
   * @param docSpecificLabels visibility labels specific to a given document.
   * @param fieldSpecificLabels visibility labels specific to a given field.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean add(BatchWriter writer, IngestStats stats, String dataset, String docId,
      String field, String term, List<Pair<Integer, Integer>> spans, Set<String> docSpecificLabels,
      Set<String> fieldSpecificLabels) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(spans, "spans should not be null");
    Preconditions.checkNotNull(docSpecificLabels, "docSpecificLabels should not be null");
    Preconditions.checkNotNull(fieldSpecificLabels, "fieldSpecificLabels should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("doc_id", docId).add("field", field).add("term", term)
          .add("doc_specific_labels", docSpecificLabels)
          .add("field_specific_labels", fieldSpecificLabels).formatDebug());
    }

    Text newTerm = new Text(term);
    Text newTermReversed = new Text(reverse(term));
    Text newField = new Text(field);
    Text newDocField = new Text(docId + Constants.SEPARATOR_NUL + field);
    Value newSpans =
        new Value(
            Integer.toString(spans.size(), 10) + Constants.SEPARATOR_NUL
                + Joiner.on(Constants.SEPARATOR_NUL)
                    .join(spans
                        .stream().map(p -> Integer.toString(p.getFirst(), 10)
                            + Constants.SEPARATOR_NUL + Integer.toString(p.getSecond(), 10))
                        .collect(Collectors.toList())));
    Value newCount = new Value(Integer.toString(spans.size(), 10));

    if (stats != null) {
      stats.count(dataset, field, spans.size());
      stats.visibility(dataset, field, fieldSpecificLabels);
      stats.visibility(dataset, field, docSpecificLabels);
    }

    // Column visibility from labels
    ColumnVisibility vizFieldSpecific =
        new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE).join(fieldSpecificLabels));
    ColumnVisibility viz = new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE)
        .join(Sets.union(docSpecificLabels, fieldSpecificLabels)));

    // Forward index
    @Var
    boolean isOk =
        add(writer, newTerm, new Text(forwardCount(dataset)), newField, vizFieldSpecific, newCount);
    isOk = isOk && add(writer, newTerm, new Text(forwardCard(dataset)), newField, vizFieldSpecific,
        Constants.VALUE_ONE);
    isOk =
        isOk && add(writer, newTerm, new Text(forwardIndex(dataset)), newDocField, viz, newSpans);

    // Backward index
    isOk = isOk && add(writer, newTermReversed, new Text(backwardCount(dataset)), newField,
        vizFieldSpecific, newCount);
    isOk = isOk && add(writer, newTermReversed, new Text(backwardCard(dataset)), newField,
        vizFieldSpecific, Constants.VALUE_ONE);
    isOk = isOk && add(writer, newTermReversed, new Text(backwardIndex(dataset)), newDocField, viz,
        newSpans);

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

      List<Range> ranges = fields.stream().map(Range::exact).collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return Constants.ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String field = key.getRow().toString();

      // Extract term count from VALUE
      long count = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new FieldCount(field, labels, count);
    });
  }

  /**
   * Get the number of documents indexed for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields.
   * @return count.
   */
  public Iterator<FieldCard> fieldCard(ScannerBase scanner, String dataset, Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(card(dataset)));

    if (fields != null) {

      List<Range> ranges = fields.stream().map(Range::exact).collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return Constants.ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String field = key.getRow().toString();

      // Extract term cardinality from VALUE
      long cardinality = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new FieldCard(field, labels, cardinality);
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

      List<Range> ranges = fields.stream().map(Range::exact).collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return Constants.ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      Key key = entry.getKey();
      Value value = entry.getValue();

      // Extract term from ROW
      String field = key.getRow().toString();

      // Extract term labels from VALUE
      Set<String> labelsTerm =
          Sets.newHashSet(Splitter.on(Constants.SEPARATOR_NUL).split(value.toString()));

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labelsAccumulo = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new FieldLabels(field, labelsAccumulo, labelsTerm);
    });
  }

  /**
   * Get the number of occurrences of each term for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param term term. Might contain wildcard characters.
   * @return an iterator sorted in lexicographic order by term.
   */
  public Iterator<Pair<String, List<TermCount>>> termCount(Scanner scanner, String dataset,
      String term) {
    return new GroupByTermIterator<>(termCount((ScannerBase) scanner, dataset, term));
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

    if (newDataset != null) {
      scanner.fetchColumnFamily(new Text(newDataset));
    } else {

      IteratorSetting setting = new IteratorSetting(22, "WildcardFilter", WildcardFilter.class);
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
      String field = key.getColumnQualifier().toString();

      // Extract count from VALUE
      long count = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new TermCount(field, termm, labels, count);
    });
  }

  /**
   * Get the number of documents of each matching term for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param term term. Might contain wildcard characters.
   * @return an iterator sorted in lexicographic order by term.
   */
  public Iterator<Pair<String, List<TermCard>>> termCard(Scanner scanner, String dataset,
      String term) {
    return new GroupByTermIterator<>(termCard((ScannerBase) scanner, dataset, term));
  }

  /**
   * Get the number of documents of each matching term for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCard> termCard(ScannerBase scanner, String dataset, String term) {

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
      newDataset = dataset == null ? null : backwardCard(dataset);
    } else {
      newTerm = term;
      newDataset = dataset == null ? null : forwardCard(dataset);
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

    if (newDataset != null) {
      scanner.fetchColumnFamily(new Text(newDataset));
    } else {

      IteratorSetting setting = new IteratorSetting(22, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnColumnFamily(setting);
      WildcardFilter.addWildcard(setting, isTermBackward ? "*_BCARD" : "*_FCARD");

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
      String field = key.getColumnQualifier().toString();

      // Extract cardinality from VALUE
      long cardinality = Long.parseLong(value.toString(), 10);

      // Extract visibility labels
      String cv = key.getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

      return new TermCard(field, termm, labels, cardinality);
    });
  }

  /**
   * Get documents, fields and spans aggregated by term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term term. Might contain wildcard characters.
   * @param keepFields fields patterns to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return an iterator sorted in lexicographic order by term.
   */
  public Iterator<Pair<String, List<Term>>> termScan(Scanner scanner, String dataset, String term,
      Set<String> keepFields, BloomFilters<String> keepDocs) {
    return new GroupByTermIterator<>(
        termScan((ScannerBase) scanner, dataset, term, keepFields, keepDocs));
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

    if (newDataset != null) {
      scanner.fetchColumnFamily(new Text(newDataset));
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
      String field = cq.substring(index + 1);

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

      return new Term(docId, field, termm, labels, count, ranges);
    });
  }
}
