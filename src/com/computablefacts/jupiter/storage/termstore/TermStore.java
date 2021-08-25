package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.combiners.TermStoreCombiner;
import com.computablefacts.jupiter.filters.TermStoreBucketFieldFilter;
import com.computablefacts.jupiter.filters.TermStoreFieldFilter;
import com.computablefacts.jupiter.filters.WildcardFilter;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.helpers.BigDecimalCodec;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
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
 *  Row                             | Column Family   | Column Qualifier                  | Visibility                                  | Value
 * =================================+=================+===================================+=============================================+=================================
 *  <dataset>\0<field>\0<term_type> | DB              | (empty)                           | ADM|<dataset>_DB                            | #distinct_buckets
 *  <dataset>\0<field>\0<term_type> | DT              | (empty)                           | ADM|<dataset>_DT                            | #distinct_terms
 *  <dataset>\0<field>\0<term_type> | LU              | (empty)                           | ADM|<dataset>_LU                            | utc_date
 *  <dataset>\0<field>\0<term_type> | TT              | (empty)                           | ADM|<dataset>_TT                            | top_k_terms
 *  <dataset>\0<field>\0<term_type> | VIZ             | (empty)                           | ADM|<dataset>_VIZ                           | viz1\0viz2\0...
 *  <dataset>\0<mret>               | BCNT            | <field>\0<term_type>              | ADM|<dataset>_<field>                       | #buckets_with_at_least_one_term_occurrence
 *  <dataset>\0<mret>               | BIDX            | <bucket_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<bucket_id> | #occurrences_of_term_in_bucket
 *  <dataset>\0<term>               | FCNT            | <field>\0<term_type>              | ADM|<dataset>_<field>                       | #buckets_with_at_least_one_term_occurrence
 *  <dataset>\0<term>               | FIDX            | <bucket_id>\0<field>\0<term_type> | ADM|<dataset>_<field>|<dataset>_<bucket_id> | #occurrences_of_term_in_bucket
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class TermStore extends AbstractStorage {

  public static final String DISTINCT_TERMS = "DT";
  public static final String DISTINCT_BUCKETS = "DB";
  public static final String TOP_TERMS = "TT";
  public static final String VISIBILITY = "VIZ";
  public static final String LAST_UPDATE = "LU";
  public static final String FORWARD_COUNT = "FCNT";
  public static final String FORWARD_INDEX = "FIDX";
  public static final String BACKWARD_COUNT = "BCNT";
  public static final String BACKWARD_INDEX = "BIDX";

  private static final int TERMSTORE_COMBINER_PRIORITY = 10;
  private static final int WILDCARD_FILTER_PRIORITY = 30;
  private static final int FIELD_FILTER_PRIORITY = 31;
  private static final int BUCKET_FIELD_FILTER_PRIORITY = 31;

  private static final Logger logger_ = LoggerFactory.getLogger(TermStore.class);

  private Map<String, ThetaSketch> fieldsCardinalityEstimatorsForTerms_;
  private Map<String, FieldDistinctBuckets> fieldsDistinctBuckets_;
  private Map<String, TopKSketch> fieldsTopTerms_;
  private Map<String, FieldLastUpdate> fieldsLastUpdate_;
  private Map<String, FieldLabels> fieldsLabels_;

  private String prevDataset_ = "";
  private String prevField_ = "";
  private String prevBucketId_ = "";

  public TermStore(Configurations configurations, String name) {
    super(configurations, name);
  }

  private static Iterator<TermDistinctBuckets> scanCounts(ScannerBase scanner, Set<String> fields,
      Range range, boolean hitsBackwardIndex) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    if (hitsBackwardIndex) {
      scanner.fetchColumnFamily(new Text(BACKWARD_COUNT));
    } else {
      scanner.fetchColumnFamily(new Text(FORWARD_COUNT));
    }
    if (fields != null && !fields.isEmpty()) {
      IteratorSetting setting = new IteratorSetting(FIELD_FILTER_PRIORITY, "TermStoreFieldFilter",
          TermStoreFieldFilter.class);
      TermStoreFieldFilter.setFieldsToKeep(setting, fields);
      scanner.addScanIterator(setting);
    }
    if (!setRange(scanner, range)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(
        Iterators.transform(scanner.iterator(),
            entry -> TermDistinctBuckets.fromKeyValue(entry.getKey(), entry.getValue())),
        tc -> new TermDistinctBuckets(tc.dataset(), tc.field(), tc.type(),
            tc.isNumber() ? BigDecimalCodec.decode(tc.term()) : tc.term(), tc.labels(),
            tc.count()));
  }

  private static Iterator<Term> scanIndex(ScannerBase scanner, Set<String> fields, Range range,
      boolean hitsBackwardIndex, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    if (hitsBackwardIndex) {
      scanner.fetchColumnFamily(new Text(BACKWARD_INDEX));
    } else {
      scanner.fetchColumnFamily(new Text(FORWARD_INDEX));
    }

    @Var
    boolean add = false;
    IteratorSetting setting = new IteratorSetting(BUCKET_FIELD_FILTER_PRIORITY,
        "TermStoreBucketFieldFilter", TermStoreBucketFieldFilter.class);

    if (fields != null && !fields.isEmpty()) {
      add = true;
      TermStoreBucketFieldFilter.setFieldsToKeep(setting, fields);
    }
    if (bucketsIds != null) {
      add = true;
      TermStoreBucketFieldFilter.setDocsToKeep(setting, bucketsIds);
    }
    if (add) {
      scanner.addScanIterator(setting);
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

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    if (!isReady()) {
      if (!super.create()) {
        return false;
      }
    }

    try {

      // Remove legacy iterators from the TermStore
      Map<String, EnumSet<IteratorUtil.IteratorScope>> iterators =
          configurations().tableOperations().listIterators(tableName());

      if (iterators.containsKey("TermStoreCombiner")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(),
            TermStoreCombiner.class.getSimpleName(), EnumSet.of(IteratorUtil.IteratorScope.majc,
                IteratorUtil.IteratorScope.minc, IteratorUtil.IteratorScope.scan));
      }

      // Set combiner
      IteratorSetting setting =
          new IteratorSetting(TERMSTORE_COMBINER_PRIORITY, TermStoreCombiner.class);
      TermStoreCombiner.setCombineAllColumns(setting, true);
      TermStoreCombiner.setReduceOnFullCompactionOnly(setting, true);

      configurations().tableOperations().attachIterator(tableName(), setting);

      // Set locality groups
      return addLocalityGroups(Sets.newHashSet(TOP_TERMS, DISTINCT_TERMS, DISTINCT_BUCKETS,
          LAST_UPDATE, VISIBILITY, FORWARD_COUNT, FORWARD_INDEX, BACKWARD_COUNT, BACKWARD_INDEX));

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  /**
   * Remove all data from the table. Existing splits are kept.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  @Override
  public boolean truncate() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    if (!super.truncate()) {
      return false;
    }

    Set<String> cfs = Sets.newHashSet(TOP_TERMS, DISTINCT_TERMS, DISTINCT_BUCKETS, LAST_UPDATE,
        VISIBILITY, FORWARD_COUNT, FORWARD_INDEX, BACKWARD_COUNT, BACKWARD_INDEX);

    return addLocalityGroups(cfs);
  }

  /**
   * Remove all data for a given dataset.
   *
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeDataset(String dataset) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .formatDebug());
    }

    String begin = dataset + SEPARATOR_NUL;
    String end =
        begin.substring(0, begin.length() - 1) + (char) (begin.charAt(begin.length() - 1) + 1);

    return Tables.deleteRows(configurations().tableOperations(), tableName(), begin, end);
  }

  /**
   * This method should be called once, at the beginning of the ingest process.
   *
   * If the {@link #beginIngest()} and {@link #endIngest(BatchWriter, String)} methods are called
   * too often, the estimators may be heavily skewed towards a subset of the data.
   */
  public void beginIngest() {
    fieldsCardinalityEstimatorsForTerms_ = new HashMap<>();
    fieldsDistinctBuckets_ = new HashMap<>();
    fieldsTopTerms_ = new HashMap<>();
    fieldsLastUpdate_ = new HashMap<>();
    fieldsLabels_ = new HashMap<>();
    prevDataset_ = "";
    prevField_ = "";
    prevBucketId_ = "";
  }

  /**
   * This method should be called once, at the end of the ingest process.
   *
   * If the {@link #beginIngest()} and {@link #endIngest(BatchWriter, String)} methods are called
   * too often, the estimators may be heavily skewed towards a subset of the data.
   *
   * @param writer batch writer.
   * @param dataset the dataset.
   * @return true if the write operations succeeded, false otherwise.
   */
  @CanIgnoreReturnValue
  public boolean endIngest(BatchWriter writer, String dataset) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    @Var
    boolean isOk = true;

    if (fieldsCardinalityEstimatorsForTerms_ != null) {
      isOk = fieldsCardinalityEstimatorsForTerms_.entrySet().stream()
          .allMatch(sketch -> add(writer, FieldDistinctTerms.newMutation(dataset, sketch.getKey(),
              sketch.getValue().toByteArray())))
          && isOk;
      fieldsCardinalityEstimatorsForTerms_ = null;
    }
    if (fieldsDistinctBuckets_ != null) {
      isOk = fieldsDistinctBuckets_.entrySet().stream()
          .allMatch(db -> add(writer,
              FieldDistinctBuckets.newMutation(dataset, db.getKey(), db.getValue().estimate())))
          && isOk;
      fieldsDistinctBuckets_ = null;
      prevDataset_ = "";
      prevField_ = "";
      prevBucketId_ = "";
    }
    if (fieldsTopTerms_ != null) {
      isOk = fieldsTopTerms_.entrySet().stream()
          .allMatch(sketch -> add(writer,
              FieldTopTerms.newMutation(dataset, sketch.getKey(), sketch.getValue().toByteArray())))
          && isOk;
      fieldsTopTerms_ = null;
    }
    if (fieldsLabels_ != null) {
      isOk =
          fieldsLabels_.values().stream()
              .allMatch(fl -> add(writer,
                  FieldLabels.newMutation(fl.dataset(), fl.field(), fl.type(), fl.labels())))
              && isOk;
      fieldsLabels_ = null;
    }
    if (fieldsLastUpdate_ != null) {
      isOk = fieldsLastUpdate_.values().stream().allMatch(
          flu -> add(writer, FieldLastUpdate.newMutation(flu.dataset(), flu.field(), flu.type())))
          && isOk;
      fieldsLastUpdate_ = null;
    }
    return isOk;
  }

  /**
   * Persist data. Term extraction for a given field from a given bucket should be performed by the
   * caller. This method should be called only once for each quad (dataset, bucketId, field, term).
   *
   * WARNING : this method makes the assumption that triples (dataset, bucketId, field) are ordered
   * and processed one after the other. For example, this sequence will skew the bucket count :
   *
   * <pre>
   *     Call n°1: (dataset_1, bucket_1, field_1)
   *     Call n°2: (dataset_1, bucket_1, field_2)
   *     Call n°3: (dataset_1, bucket_1, field_1)
   * </pre>
   *
   * but this sequence is will not :
   *
   * <pre>
   *     Call n°1: (dataset_1, bucket_1, field_1)
   *     Call n°2: (dataset_1, bucket_1, field_1)
   *     Call n°3: (dataset_1, bucket_1, field_2)
   * </pre>
   *
   * @param writer batch writer.
   * @param dataset the dataset.
   * @param bucketId the bucket id.
   * @param field the field name.
   * @param term the term to index.
   * @param nbOccurrences the number of occurrences of the term in the bucket.
   * @param bucketSpecificLabels the visibility labels specific to a given bucket.
   * @param fieldSpecificLabels the visibility labels specific to a given field.
   * @return true if the write operation succeeded, false otherwise.
   */
  public boolean put(BatchWriter writer, String dataset, String bucketId, String field, Object term,
      int nbOccurrences, Set<String> bucketSpecificLabels, Set<String> fieldSpecificLabels) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(bucketId, "bucketId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrences > 0, "nbOccurrences must be > 0");
    Preconditions.checkNotNull(bucketSpecificLabels, "bucketSpecificLabels should not be null");
    Preconditions.checkNotNull(fieldSpecificLabels, "fieldSpecificLabels should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("bucket_id", bucketId).add("field", field).add("term", term)
          .add("nb_occurrences", nbOccurrences).add("bucket_specific_labels", bucketSpecificLabels)
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
      logger_.warn(LogFormatter.create(true)
          .message(String.format(
              "%s has been lexicoded to null/an empty string. Term has been ignored.", term))
          .formatWarn());
      return false;
    }

    // Compute the number of distinct terms
    if (fieldsCardinalityEstimatorsForTerms_ != null) {

      String key = field + SEPARATOR_NUL + newType;

      if (!fieldsCardinalityEstimatorsForTerms_.containsKey(key)) {
        fieldsCardinalityEstimatorsForTerms_.put(key, new ThetaSketch());
      }
      fieldsCardinalityEstimatorsForTerms_.get(key).offer(newTerm);
    }

    // Compute the number of distinct buckets
    if (fieldsDistinctBuckets_ != null && (!prevDataset_.equals(dataset)
        || !prevField_.equals(field) || !prevBucketId_.equals(bucketId))) {

      String key = field + SEPARATOR_NUL + Term.TYPE_NA;

      if (!fieldsDistinctBuckets_.containsKey(key)) {
        fieldsDistinctBuckets_.put(key,
            new FieldDistinctBuckets(dataset, field, newType, fieldSpecificLabels, 0));
      }

      FieldDistinctBuckets prev = fieldsDistinctBuckets_.get(key);
      FieldDistinctBuckets next = new FieldDistinctBuckets(dataset, field, newType,
          fieldSpecificLabels, prev.estimate() + 1);

      fieldsDistinctBuckets_.put(key, next);

      prevDataset_ = dataset;
      prevField_ = field;
      prevBucketId_ = bucketId;
    }

    // Compute the top k terms
    if (fieldsTopTerms_ != null) {

      String key = field + SEPARATOR_NUL + newType;

      if (!fieldsTopTerms_.containsKey(key)) {
        fieldsTopTerms_.put(key, new TopKSketch());
      }
      fieldsTopTerms_.get(key).offer(term instanceof Number ? term.toString() : newTerm,
          nbOccurrences);
    }

    // Compute last update
    if (fieldsLastUpdate_ != null) {

      String key = field + SEPARATOR_NUL + newType;
      FieldLastUpdate fieldLastUpdate =
          new FieldLastUpdate(dataset, field, newType, fieldSpecificLabels);

      fieldsLastUpdate_.put(key, fieldLastUpdate); // Replace the previous entry (if any)
    }

    // Compute visibility labels
    if (fieldsLabels_ != null) {

      String key = field + SEPARATOR_NUL + newType;
      FieldLabels fieldLabels = new FieldLabels(dataset, field, newType, fieldSpecificLabels);

      if (!fieldsLabels_.containsKey(key)) {
        fieldsLabels_.put(key, fieldLabels);
      }

      FieldLabels prev = fieldsLabels_.get(key);

      if (!fieldLabels.equals(prev)) {

        Preconditions.checkState(dataset.equals(prev.dataset()),
            "mismatch between datasets : %s expected but %s found", dataset, prev.dataset());
        Preconditions.checkState(fieldLabels.termLabels().equals(prev.termLabels()),
            "mismatch between term labels : %s expected but %s found", fieldLabels.termLabels(),
            prev.termLabels());

        Set<String> labels = Sets.union(fieldSpecificLabels, prev.labels());
        FieldLabels newFieldLabels = new FieldLabels(dataset, field, newType, labels);

        fieldsLabels_.put(key, newFieldLabels); // Replace the previous entry (if any)
      }
    }

    Map<Text, Mutation> mutations = new HashMap<>();

    // Forward index
    TermDistinctBuckets.newForwardMutation(mutations, dataset, field, newType, newTerm, 1,
        fieldSpecificLabels);
    Term.newForwardMutation(mutations, dataset, bucketId, field, newType, newTerm, nbOccurrences,
        Sets.union(bucketSpecificLabels, fieldSpecificLabels));

    if (!writeInForwardIndexOnly) {

      // Backward index
      TermDistinctBuckets.newBackwardMutation(mutations, dataset, field, newType, newTerm, 1,
          fieldSpecificLabels);
      Term.newBackwardMutation(mutations, dataset, bucketId, field, newType, newTerm, nbOccurrences,
          Sets.union(bucketSpecificLabels, fieldSpecificLabels));
    }
    return mutations.values().stream().allMatch(v -> add(writer, v));
  }

  /**
   * Get the visibility labels associated to each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
   * @return visibility labels.
   */
  public Iterator<FieldLabels> fieldVisibilityLabels(ScannerBase scanner, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(VISIBILITY));

    List<Range> ranges;

    if (fields != null && !fields.isEmpty()) {
      ranges = fields.stream()
          .map(field -> Range.prefix(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL))
          .collect(Collectors.toList());
    } else {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    }
    if (!setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
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

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(LAST_UPDATE));

    List<Range> ranges;

    if (fields != null && !fields.isEmpty()) {
      ranges = fields.stream()
          .map(field -> Range.prefix(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL))
          .collect(Collectors.toList());
    } else {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    }
    if (!setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldLastUpdate.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * Get the number of distinct terms in each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
   * @return the number of distinct terms.
   */
  public Iterator<FieldDistinctTerms> fieldCardinalityEstimationForTerms(ScannerBase scanner,
      String dataset, Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(DISTINCT_TERMS));

    List<Range> ranges;

    if (fields != null && !fields.isEmpty()) {
      ranges = fields.stream()
          .map(field -> Range.prefix(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL))
          .collect(Collectors.toList());
    } else {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    }
    if (!setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldDistinctTerms.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * Get the number of distinct buckets in each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
   * @return the number of distinct buckets.
   */
  public Iterator<FieldDistinctBuckets> fieldCardinalityEstimationForBuckets(ScannerBase scanner,
      String dataset, Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(DISTINCT_BUCKETS));

    List<Range> ranges;

    if (fields != null && !fields.isEmpty()) {
      ranges = fields.stream()
          .map(field -> Range.prefix(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL))
          .collect(Collectors.toList());
    } else {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    }
    if (!setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldDistinctBuckets.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * Get the most frequent terms in each field.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields fields (optional).
   * @return the number of distinct buckets.
   */
  public Iterator<FieldTopTerms> fieldTopTerms(ScannerBase scanner, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(TOP_TERMS));

    List<Range> ranges;

    if (fields != null && !fields.isEmpty()) {
      ranges = fields.stream()
          .map(field -> Range.prefix(dataset + SEPARATOR_NUL + field + SEPARATOR_NUL))
          .collect(Collectors.toList());
    } else {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    }
    if (!setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(),
        entry -> FieldTopTerms.fromKeyValue(entry.getKey(), entry.getValue()));
  }

  /**
   * For each field in a given dataset, get the number of buckets with at least one occurrence of a
   * given term.
   *
   * @param scanner scanner.
   * @param dataset dataset (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermDistinctBuckets> termCardinalityEstimationForBuckets(ScannerBase scanner,
      String dataset, String term) {
    return termCardinalityEstimationForBuckets(scanner, dataset, null, term);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of buckets with at least one
   * occurrence of a given term.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermDistinctBuckets> termCardinalityEstimationForBuckets(ScannerBase scanner,
      String dataset, Set<String> fields, String term) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).add("term", term).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);
    String newTerm = isTermBackward ? reverse(term) : term;

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      range = Range.exact(dataset + SEPARATOR_NUL + newTerm,
          isTermBackward ? BACKWARD_COUNT : FORWARD_COUNT);
    } else {

      range = Range.prefix(dataset + SEPARATOR_NUL + WildcardMatcher.prefix(newTerm));

      IteratorSetting setting =
          new IteratorSetting(WILDCARD_FILTER_PRIORITY, "WildcardFilter1", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, dataset + SEPARATOR_NUL + newTerm);

      scanner.addScanIterator(setting);
    }
    return scanCounts(scanner, fields, range, isTermBackward);
  }

  /**
   * For each field in a given dataset, get the ones matching a given term.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param term searched term. Might contain wildcard characters.
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of a
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
   * @param dataset dataset.
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param bucketsIds which buckets must be considered (optional).
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> bucketsIds(ScannerBase scanner, String dataset, Set<String> fields,
      String term, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).add("term", term).add("has_buckets_ids", bucketsIds != null)
          .formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);
    String newTerm = isTermBackward ? reverse(term) : term;

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      range = Range.exact(dataset + SEPARATOR_NUL + newTerm,
          isTermBackward ? BACKWARD_INDEX : FORWARD_INDEX);
    } else {

      range = Range.prefix(dataset + SEPARATOR_NUL + WildcardMatcher.prefix(newTerm));

      IteratorSetting setting =
          new IteratorSetting(WILDCARD_FILTER_PRIORITY, "WildcardFilter1", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, dataset + SEPARATOR_NUL + newTerm);

      scanner.addScanIterator(setting);
    }
    return scanIndex(scanner, fields, range, isTermBackward, bucketsIds);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of occurrences of all terms in
   * [minTerm, maxTerm]. Note that this method only hits the forward index.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<TermDistinctBuckets> termCardinalityEstimationForBuckets(ScannerBase scanner,
      String dataset, Set<String> fields, Object minTerm, Object maxTerm) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).add("min_term", minTerm).add("max_term", maxTerm).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    Key beginKey;
    Key endKey;

    if ((minTerm == null || minTerm instanceof String)
        && (maxTerm == null || maxTerm instanceof String)) {

      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + minTerm);
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + maxTerm);

      Preconditions.checkState(minTerm == null || !WildcardMatcher.hasWildcards((String) minTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(maxTerm == null || !WildcardMatcher.hasWildcards((String) maxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      beginKey = minTerm == null ? null
          : new Key(dataset + SEPARATOR_NUL + Codecs.defaultLexicoder.apply(minTerm).text());
      endKey = maxTerm == null ? null
          : new Key(dataset + SEPARATOR_NUL + Codecs.defaultLexicoder.apply(maxTerm).text());
    }

    Range range = new Range(beginKey, endKey);
    return scanCounts(scanner, fields, range, false);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get buckets having a term in
   * [minTerm, maxTerm]. Note that this method only hits the forward index.
   *
   * @param scanner scanner.
   * @param dataset dataset.
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param bucketsIds which buckets must be considered (optional).
   * @return an iterator whose entries are sorted by term if and only if {@link ScannerBase} is an
   *         instance of a {@link org.apache.accumulo.core.client.Scanner} instead of a
   *         {@link org.apache.accumulo.core.client.BatchScanner}.
   */
  public Iterator<Term> bucketsIds(ScannerBase scanner, String dataset, Set<String> fields,
      Object minTerm, Object maxTerm, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("fields", fields).add("min_term", minTerm).add("max_term", maxTerm)
          .add("has_buckets_ids", bucketsIds != null).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    Key beginKey;
    Key endKey;

    if ((minTerm == null || minTerm instanceof String)
        && (maxTerm == null || maxTerm instanceof String)) {

      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + minTerm);
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + maxTerm);

      Preconditions.checkState(minTerm == null || !WildcardMatcher.hasWildcards((String) minTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(maxTerm == null || !WildcardMatcher.hasWildcards((String) maxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      beginKey = minTerm == null ? null
          : new Key(dataset + SEPARATOR_NUL + Codecs.defaultLexicoder.apply(minTerm).text());
      endKey = maxTerm == null ? null
          : new Key(dataset + SEPARATOR_NUL + Codecs.defaultLexicoder.apply(maxTerm).text());
    }

    Range range = new Range(beginKey, endKey);
    return scanIndex(scanner, fields, range, false, bucketsIds);
  }
}
