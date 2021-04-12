package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.Date;
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
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
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
import com.computablefacts.nona.helpers.BigDecimalCodec;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * The TermStore API allows your application to persist buckets of key-value pairs. Numbers and
 * dates are automatically lexicoded to maintain their native Java sort order.
 * </p>
 * 
 * <p>
 * This storage layer utilizes the <a href="https://accumulo.apache.org">Accumulo</a> table schemas
 * described below as the basis for its ingest and query components.
 * </p>
 *
 * <pre>
 *  Row                     | Column Family   | Column Qualifier                  | Visibility                                  | Value
 * =========================+=================+===================================+=============================================+=================================
 *  <field>\0<term_type>    | <dataset>_CNT   | (empty)                           | ADM|<dataset>_CNT                           | <#occurrences>
 *  <field>\0<term_type>    | <dataset>_LU    | (empty)                           | ADM|<dataset>_LU                            | <utc_date>
 *  <field>\0<term_type>    | <dataset>_VIZ   | (empty)                           | ADM|<dataset>_VIZ                           | viz1\0viz2\0
 *  <mret>                  | <dataset>_BCNT  | <field>\0<term_type>              | ADM|<dataset>_<field>                       | <#occurrences>
 *  <mret>                  | <dataset>_BIDX  | <bucket_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<bucket_id> | <#occurrences>
 *  <term>                  | <dataset>_FCNT  | <field>\0<term_type>              | ADM|<dataset>_<field>                       | <#occurrences>
 *  <term>                  | <dataset>_FIDX  | <bucket_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<bucket_id> | <#occurrences>
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

  static String lastUpdate(String dataset) {
    return dataset + "_LU";
  }

  static String forwardCount(String dataset) {
    return dataset + "_FCNT";
  }

  static String forwardIndex(String dataset) {
    return dataset + "_FIDX";
  }

  static String backwardCount(String dataset) {
    return dataset + "_BCNT";
  }

  static String backwardIndex(String dataset) {
    return dataset + "_BIDX";
  }

  private static Iterator<TermCount> scanCounts(ScannerBase scanner, String dataset,
      Set<String> fields, Range range, boolean hitsBackwardIndex) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    if (fields != null && !fields.isEmpty()) {
      IteratorSetting setting =
          new IteratorSetting(22, "TermStoreFieldFilter", TermStoreFieldFilter.class);
      TermStoreFieldFilter.setFieldsToKeep(setting, fields);
      scanner.addScanIterator(setting);
    }

    if (dataset != null) {
      scanner.fetchColumnFamily(new Text(dataset));
    } else {

      IteratorSetting setting = new IteratorSetting(23, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnColumnFamily(setting);
      WildcardFilter.addWildcard(setting, hitsBackwardIndex ? "*_BCNT" : "*_FCNT");

      scanner.addScanIterator(setting);
    }
    if (!setRange(scanner, range)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(
        Iterators.transform(scanner.iterator(),
            entry -> TermCount.fromKeyValue(entry.getKey(), entry.getValue())),
        tc -> new TermCount(tc.dataset(), tc.field(), tc.type(),
            tc.isNumber() ? BigDecimalCodec.decode(tc.term()) : tc.term(), tc.labels(),
            tc.count()));
  }

  private static Iterator<Term> scanIndex(ScannerBase scanner, String dataset, Set<String> fields,
      Range range, boolean hitsBackwardIndex, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    @Var
    boolean add = false;
    IteratorSetting setting =
        new IteratorSetting(22, "TermStoreDocFieldFilter", TermStoreDocFieldFilter.class);

    if (fields != null && !fields.isEmpty()) {
      add = true;
      TermStoreDocFieldFilter.setFieldsToKeep(setting, fields);
    }
    if (bucketsIds != null) {
      add = true;
      TermStoreDocFieldFilter.setDocsToKeep(setting, bucketsIds);
    }
    if (add) {
      scanner.addScanIterator(setting);
    }

    if (dataset != null) {
      scanner.fetchColumnFamily(new Text(dataset));
    } else {

      IteratorSetting settings = new IteratorSetting(23, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnColumnFamily(settings);
      WildcardFilter.addWildcard(settings, hitsBackwardIndex ? "*_BIDX" : "*_FIDX");

      scanner.addScanIterator(settings);
    }
    if (!setRange(scanner, range)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(
        Iterators.transform(scanner.iterator(),
            entry -> Term.fromKeyValue(entry.getKey(), entry.getValue())),
        t -> new Term(t.dataset(), t.bucketId(), t.field(), t.type(),
            t.isNumber() ? BigDecimalCodec.decode(t.term()) : t.term(), t.labels(), t.count()));
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

          // Set default splits on [a-zA-Z0-9]
          SortedSet<Text> splits = new TreeSet<>();

          for (char i = '0'; i < '9' + 1; i++) {
            splits.add(new Text(Character.toString(i)));
          }

          for (char i = 'a'; i < 'z' + 1; i++) {
            splits.add(new Text(Character.toString(i)));
          }

          for (char i = 'A'; i < 'Z' + 1; i++) {
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

      // Set default splits on [a-zA-Z0-9]
      SortedSet<Text> splits = new TreeSet<>();

      for (char i = '0'; i < '9' + 1; i++) {
        splits.add(new Text(Character.toString(i)));
      }

      for (char i = 'a'; i < 'z' + 1; i++) {
        splits.add(new Text(Character.toString(i)));
      }

      for (char i = 'A'; i < 'Z' + 1; i++) {
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

    Set<String> cfs = Sets.newHashSet(count(dataset), lastUpdate(dataset), visibility(dataset),
        forwardCount(dataset), forwardIndex(dataset), backwardCount(dataset),
        backwardIndex(dataset));

    return remove(deleter, cfs);
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
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
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
   * Persist data. Term extraction for a given field from a given bucket should be performed by the
   * caller. This method should be called only once for each quad (dataset, bucketId, field, term).
   *
   * @param writer batch writer.
   * @param dataset the dataset.
   * @param bucketId the bucket id.
   * @param field the field name.
   * @param term the term to index.
   * @param nbOccurrencesInBucket the number of occurrences of the term in the bucket.
   * @param bucketSpecificLabels the visibility labels specific to a given bucket.
   * @param fieldSpecificLabels the visibility labels specific to a given field.
   * @return true if the write operation succeeded, false otherwise.
   */
  public boolean put(BatchWriter writer, String dataset, String bucketId, String field, Object term,
      int nbOccurrencesInBucket, Set<String> bucketSpecificLabels,
      Set<String> fieldSpecificLabels) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(bucketId, "bucketId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrencesInBucket > 0, "nbOccurrencesInBucket must be > 0");
    Preconditions.checkNotNull(bucketSpecificLabels, "bucketSpecificLabels should not be null");
    Preconditions.checkNotNull(fieldSpecificLabels, "fieldSpecificLabels should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("bucket_id", bucketId).add("field", field).add("term", term)
          .add("nb_occurrences_in_bucket", nbOccurrencesInBucket)
          .add("bucket_specific_labels", bucketSpecificLabels)
          .add("field_specific_labels", fieldSpecificLabels).formatDebug());
    }

    @Var
    String newTerm;
    @Var
    int newType;
    @Var
    boolean writeInForwardIndexOnly;

    if (term instanceof String) {
      newTerm = (String) term;
      newType = Term.TYPE_STRING;
      writeInForwardIndexOnly = false;
    } else { // Objects other than String are lexicoded
      newTerm = Codecs.defaultLexicoder.apply(term).text();
      if (term instanceof Number) {
        newType = Term.TYPE_NUMBER;
      } else if (term instanceof Boolean) {
        newType = Term.TYPE_BOOLEAN;
      } else if (term instanceof Date) {
        newType = Term.TYPE_DATE;
      } else {
        newType = Term.TYPE_UNKNOWN;
      }
      writeInForwardIndexOnly = true;
    }

    if (com.google.common.base.Strings.isNullOrEmpty(newTerm)) {
      logger_
          .warn(LogFormatterManager.logFormatter()
              .message(String.format(
                  "%s has been lexicoded to null/an empty string. Term has been ignored.",
                  term.toString()))
              .formatWarn());
      return false;
    }

    // Ingest stats
    @Var
    boolean isOk =
        add(writer, FieldCount.newMutation(dataset, field, newType, nbOccurrencesInBucket));
    isOk = isOk && add(writer, FieldLastUpdate.newMutation(dataset, field, newType));
    isOk =
        isOk && add(writer, FieldLabels.newMutation(dataset, field, newType, fieldSpecificLabels));

    // Forward index
    isOk = isOk && add(writer, TermCount.newForwardMutation(dataset, field, newType, newTerm,
        nbOccurrencesInBucket, fieldSpecificLabels));
    isOk = isOk && add(writer, Term.newForwardMutation(dataset, bucketId, field, newType, newTerm,
        nbOccurrencesInBucket, Sets.union(bucketSpecificLabels, fieldSpecificLabels)));

    if (!writeInForwardIndexOnly) {

      // Backward index
      isOk = isOk && add(writer, TermCount.newBackwardMutation(dataset, field, newType, newTerm,
          nbOccurrencesInBucket, fieldSpecificLabels));
      isOk = isOk && add(writer, Term.newBackwardMutation(dataset, bucketId, field, newType,
          newTerm, nbOccurrencesInBucket, Sets.union(bucketSpecificLabels, fieldSpecificLabels)));
    }
    return isOk;
  }

  /**
   * Get the number of terms indexed for each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
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

    if (fields != null && !fields.isEmpty()) {

      List<Range> ranges = fields.stream().map(field -> field + SEPARATOR_NUL).map(Range::prefix)
          .collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldCount.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * Get the visibility labels associated to each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
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

    if (fields != null && !fields.isEmpty()) {

      List<Range> ranges = fields.stream().map(field -> field + SEPARATOR_NUL).map(Range::prefix)
          .collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldLabels.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * Get the date of last update associated to each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
   * @return last update as an UTC timestamp.
   */
  public Iterator<FieldLastUpdate> fieldLastUpdate(ScannerBase scanner, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(lastUpdate(dataset)));

    if (fields != null && !fields.isEmpty()) {

      List<Range> ranges = fields.stream().map(field -> field + SEPARATOR_NUL).map(Range::prefix)
          .collect(Collectors.toList());

      if (!setRanges(scanner, ranges)) {
        return ITERATOR_EMPTY;
      }
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldLastUpdate.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * For each field in a given dataset, get the number of occurrences of a given term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCount> counts(ScannerBase scanner, String dataset, String term) {
    return counts(scanner, dataset, null, term);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of occurrences of a given
   * term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCount> counts(ScannerBase scanner, String dataset, Set<String> fields,
      String term) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).add("term", term).formatInfo());
    }

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
    return scanCounts(scanner, newDataset, fields, range, isTermBackward);
  }

  /**
   * For each field in a given dataset, get the ones matching a given term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> bucketsIds(ScannerBase scanner, String dataset, String term) {
    return bucketsIds(scanner, dataset, null, term, null);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get the ones matching a given
   * term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param bucketsIds which buckets must be considered (optional).
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> bucketsIds(ScannerBase scanner, String dataset, Set<String> fields,
      String term, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).add("term", term)
          .add("has_buckets_ids", bucketsIds != null).formatInfo());
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
    return scanIndex(scanner, newDataset, fields, range, isTermBackward, bucketsIds);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of occurrences of all terms in
   * [minTerm, maxTerm]. Note that this method only hits the forward index.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermCount> counts(ScannerBase scanner, String dataset, Set<String> fields,
      Object minTerm, Object maxTerm) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).add("min_term", minTerm)
          .add("max_term", maxTerm).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    String newMinTerm;
    String newMaxTerm;

    if ((minTerm == null || minTerm instanceof String)
        && (maxTerm == null || maxTerm instanceof String)) {

      newMinTerm = (String) minTerm;
      newMaxTerm = (String) maxTerm;

      Preconditions.checkState(newMinTerm == null || !WildcardMatcher.hasWildcards(newMinTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(newMaxTerm == null || !WildcardMatcher.hasWildcards(newMaxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      newMinTerm = minTerm == null ? null : Codecs.defaultLexicoder.apply(minTerm).text();
      newMaxTerm = maxTerm == null ? null : Codecs.defaultLexicoder.apply(maxTerm).text();
    }

    Range range;

    if (newMinTerm == null) { // scan ]-inf, maxTerm]
      Key endKey = new Key(newMaxTerm);
      range = new Range(null, endKey);
    } else if (newMaxTerm == null) { // scan [minTerm, +inf[
      Key startKey = new Key(newMinTerm);
      range = new Range(startKey, null);
    } else { // scan [minTerm, maxTerm]
      Key startKey = new Key(newMinTerm);
      Key endKey = new Key(newMaxTerm);
      range = new Range(startKey, endKey);
    }

    String newDataset = dataset == null ? null : forwardCount(dataset);

    return scanCounts(scanner, newDataset, fields, range, false);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get buckets having a term in
   * [minTerm, maxTerm]. Note that this method only hits the forward index.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param bucketsIds which buckets must be considered (optional).
   * @return an iterator whose entries are sorted if and only if {@link ScannerBase} is an instance
   *         of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> bucketsIds(ScannerBase scanner, String dataset, Set<String> fields,
      Object minTerm, Object maxTerm, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("fields", fields).add("min_term", minTerm)
          .add("max_term", maxTerm).add("has_buckets_ids", bucketsIds != null).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    String newMinTerm;
    String newMaxTerm;

    if ((minTerm == null || minTerm instanceof String)
        && (maxTerm == null || maxTerm instanceof String)) {

      newMinTerm = (String) minTerm;
      newMaxTerm = (String) maxTerm;

      Preconditions.checkState(newMinTerm == null || !WildcardMatcher.hasWildcards(newMinTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(newMaxTerm == null || !WildcardMatcher.hasWildcards(newMaxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      newMinTerm = minTerm == null ? null : Codecs.defaultLexicoder.apply(minTerm).text();
      newMaxTerm = maxTerm == null ? null : Codecs.defaultLexicoder.apply(maxTerm).text();
    }

    Range range;

    if (newMinTerm == null) { // scan ]-inf, maxTerm]
      Key endKey = new Key(newMaxTerm);
      range = new Range(null, endKey);
    } else if (newMaxTerm == null) { // scan [minTerm, +inf[
      Key startKey = new Key(newMinTerm);
      range = new Range(startKey, null);
    } else { // scan [minTerm, maxTerm]
      Key startKey = new Key(newMinTerm);
      Key endKey = new Key(newMaxTerm);
      range = new Range(startKey, endKey);
    }

    String newDataset = dataset == null ? null : forwardIndex(dataset);

    return scanIndex(scanner, newDataset, fields, range, false, bucketsIds);
  }
}
