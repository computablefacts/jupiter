package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import com.computablefacts.asterix.View;
import com.computablefacts.asterix.WildcardMatcher;
import com.computablefacts.asterix.codecs.BigDecimalCodec;
import com.computablefacts.asterix.codecs.StringCodec;
import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.OrderedView;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.UnorderedView;
import com.computablefacts.jupiter.combiners.TermStoreCombiner;
import com.computablefacts.jupiter.filters.TermStoreBucketFieldFilter;
import com.computablefacts.jupiter.filters.TermStoreFieldFilter;
import com.computablefacts.jupiter.filters.WildcardFilter;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The TermStore API allows your application to persist buckets of key-value pairs. Numbers and dates are automatically
 * lexicoded to maintain their native Java sort order.
 * </p>
 *
 * <p>
 * This storage layer utilizes the <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as
 * the basis for its ingest and query components.
 * </p>
 *
 * <pre>
 *  Row                             | Column Family   | Column Qualifier                  | Visibility                                  | Value
 * =================================+=================+===================================+=============================================+=================================
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

  public static final String FORWARD_COUNT = "FCNT";
  public static final String FORWARD_INDEX = "FIDX";
  public static final String BACKWARD_COUNT = "BCNT";
  public static final String BACKWARD_INDEX = "BIDX";

  private static final int TERMSTORE_COMBINER_PRIORITY = 10;
  private static final int WILDCARD_FILTER_PRIORITY = 30;
  private static final int FIELD_FILTER_PRIORITY = 31;
  private static final int BUCKET_FIELD_FILTER_PRIORITY = 31;

  private static final Logger logger_ = LoggerFactory.getLogger(TermStore.class);

  public TermStore(Configurations configurations, String name) {
    super(configurations, name);
  }

  private static View<TermDistinctBuckets> scanCounts(ScannerBase scanner, Set<String> fields, Range range,
      boolean hitsBackwardIndex) {

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
      return View.of();
    }

    View<Map.Entry<Key, Value>> view;

    if (scanner instanceof BatchScanner) {
      view = new UnorderedView<>((BatchScanner) scanner, s -> s.iterator());
    } else {
      view = new OrderedView<>((Scanner) scanner, s -> s.iterator());
    }
    return view.map(entry -> TermDistinctBuckets.fromKeyValue(entry.getKey(), entry.getValue())).map(
        tc -> new TermDistinctBuckets(tc.dataset(), tc.field(), tc.type(),
            tc.isNumber() ? BigDecimalCodec.decode(tc.term()) : tc.term(), tc.labels(), tc.count()));
  }

  private static View<Term> scanIndex(ScannerBase scanner, Set<String> fields, Range range, boolean hitsBackwardIndex,
      BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(range, "range should not be null");

    if (hitsBackwardIndex) {
      scanner.fetchColumnFamily(new Text(BACKWARD_INDEX));
    } else {
      scanner.fetchColumnFamily(new Text(FORWARD_INDEX));
    }

    @Var boolean add = false;
    IteratorSetting setting = new IteratorSetting(BUCKET_FIELD_FILTER_PRIORITY, "TermStoreBucketFieldFilter",
        TermStoreBucketFieldFilter.class);

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
      return View.of();
    }

    View<Map.Entry<Key, Value>> view;

    if (scanner instanceof BatchScanner) {
      view = new UnorderedView<>((BatchScanner) scanner, s -> s.iterator());
    } else {
      view = new OrderedView<>((Scanner) scanner, s -> s.iterator());
    }
    return view.map(entry -> Term.fromKeyValue(entry.getKey(), entry.getValue())).map(
        t -> new Term(t.dataset(), t.bucketId(), t.field(), t.type(),
            t.isNumber() ? BigDecimalCodec.decode(t.term()) : t.term(), t.labels(), t.count()));
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false otherwise.
   */
  @Override
  public boolean create() {

    if (!isReady()) {
      if (!super.create()) {
        return false;
      }
    }

    try {

      // Remove legacy iterators from the TermStore
      Map<String, EnumSet<IteratorUtil.IteratorScope>> iterators = configurations().tableOperations()
          .listIterators(tableName());

      if (iterators.containsKey("TermStoreCombiner")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(), TermStoreCombiner.class.getSimpleName(),
            EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc,
                IteratorUtil.IteratorScope.scan));
      }

      // Set combiner
      IteratorSetting setting = new IteratorSetting(TERMSTORE_COMBINER_PRIORITY, TermStoreCombiner.class);
      TermStoreCombiner.setCombineAllColumns(setting, true);
      TermStoreCombiner.setReduceOnFullCompactionOnly(setting, true);

      configurations().tableOperations().attachIterator(tableName(), setting);

      // Set locality groups
      return addLocalityGroups(Sets.newHashSet(FORWARD_COUNT, FORWARD_INDEX, BACKWARD_COUNT, BACKWARD_INDEX));

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

    if (!super.truncate()) {
      return false;
    }

    Set<String> cfs = Sets.newHashSet(FORWARD_COUNT, FORWARD_INDEX, BACKWARD_COUNT, BACKWARD_INDEX);

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

    String begin = dataset + SEPARATOR_NUL;
    String end = begin.substring(0, begin.length() - 1) + (char) (begin.charAt(begin.length() - 1) + 1);

    return Tables.deleteRows(configurations().tableOperations(), tableName(), begin, end);
  }

  /**
   * Persist data. Term extraction for a given field from a given bucket should be performed by the caller. This method
   * should be called only once for each quad ({@code dataset}, {@code bucketId}, {@code field}, {@code term}).
   *
   * @param writer               batch writer.
   * @param dataset              the dataset.
   * @param bucketId             the bucket id.
   * @param field                the field name.
   * @param term                 the term to index.
   * @param nbOccurrences        the number of occurrences of the term in the bucket.
   * @param bucketSpecificLabels the visibility labels specific to a given bucket.
   * @param fieldSpecificLabels  the visibility labels specific to a given field.
   * @return true if the write operation succeeded, false otherwise.
   */
  public boolean put(BatchWriter writer, String dataset, String bucketId, String field, Object term, int nbOccurrences,
      Set<String> bucketSpecificLabels, Set<String> fieldSpecificLabels) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(bucketId, "bucketId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrences > 0, "nbOccurrences must be > 0");
    Preconditions.checkNotNull(bucketSpecificLabels, "bucketSpecificLabels should not be null");
    Preconditions.checkNotNull(fieldSpecificLabels, "fieldSpecificLabels should not be null");

    @Var String newTerm;
    @Var int newType;
    @Var boolean writeInForwardIndexOnly;

    if (term instanceof String) {
      newTerm = (String) term;
      newType = Term.TYPE_STRING;
      writeInForwardIndexOnly = false;
    } else { // Objects other than String are lexicoded
      newTerm = StringCodec.defaultLexicoder(term);
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
          .message(String.format("%s has been lexicoded to null/an empty string. Term has been ignored.", term))
          .formatWarn());
      return true;
    }

    Map<Text, Mutation> mutations = new HashMap<>();

    // Forward index
    TermDistinctBuckets.newForwardMutation(mutations, dataset, field, newType, newTerm, 1, fieldSpecificLabels);
    Term.newForwardMutation(mutations, dataset, bucketId, field, newType, newTerm, nbOccurrences,
        Sets.union(bucketSpecificLabels, fieldSpecificLabels));

    if (!writeInForwardIndexOnly) {

      // Backward index
      TermDistinctBuckets.newBackwardMutation(mutations, dataset, field, newType, newTerm, 1, fieldSpecificLabels);
      Term.newBackwardMutation(mutations, dataset, bucketId, field, newType, newTerm, nbOccurrences,
          Sets.union(bucketSpecificLabels, fieldSpecificLabels));
    }
    return add(writer, mutations.values());
  }

  /**
   * For each field in a given dataset, get the number of buckets with at least one occurrence of a given {@code term}
   * (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset (optional).
   * @param term           searched term. Might contain wildcard characters.
   */
  public View<TermDistinctBuckets> termCardinalityEstimationForBuckets(Authorizations authorizations, String dataset,
      String term) {
    return termCardinalityEstimationForBuckets(authorizations, dataset, null, term);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of buckets with at least one occurrence of a given
   * {@code term} (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param term           searched term. Might contain wildcard characters.
   */
  public View<TermDistinctBuckets> termCardinalityEstimationForBuckets(Authorizations authorizations, String dataset,
      Set<String> fields, String term) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(!(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    BatchScanner scanner = batchScanner(compact(authorizations, dataset, null), NB_QUERY_THREADS);
    scanner.clearColumns();
    scanner.clearScanIterators();

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);
    String newTerm = isTermBackward ? new StringBuilder(term).reverse().toString() : term;

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      range = Range.exact(dataset + SEPARATOR_NUL + newTerm, isTermBackward ? BACKWARD_COUNT : FORWARD_COUNT);
    } else {

      range = Range.prefix(dataset + SEPARATOR_NUL + WildcardMatcher.prefix(newTerm));

      IteratorSetting setting = new IteratorSetting(WILDCARD_FILTER_PRIORITY, "WildcardFilter1", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, dataset + SEPARATOR_NUL + newTerm);

      scanner.addScanIterator(setting);
    }
    return scanCounts(scanner, fields, range, isTermBackward);
  }

  /**
   * For each field in a given dataset, get the ones matching a given {@code term} (sorted).
   *
   * @param authorizations authorizations
   * @param dataset        dataset.
   * @param term           searched term. Might contain wildcard characters.
   */
  public View<Term> termsSortedByTermAndBucketId(Authorizations authorizations, String dataset, String term) {
    return termsSortedByTermAndBucketId(authorizations, dataset, null, term, null);
  }

  /**
   * For each field in a given dataset, get the ones matching a given {@code term} (unsorted).
   *
   * @param authorizations authorizations
   * @param dataset        dataset.
   * @param term           searched term. Might contain wildcard characters.
   */
  public View<Term> terms(Authorizations authorizations, String dataset, String term) {
    return terms(authorizations, dataset, null, term, null);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get the ones matching a given {@code term} (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param term           searched term. Might contain wildcard characters.
   * @param bucketsIds     which buckets must be considered (optional).
   */
  public View<Term> termsSortedByTermAndBucketId(Authorizations authorizations, String dataset, Set<String> fields,
      String term, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return terms(scanner(compact(authorizations, dataset, null)), dataset, fields, term, bucketsIds);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get the ones matching a given {@code term}
   * (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param term           searched term. Might contain wildcard characters.
   * @param bucketsIds     which buckets must be considered (optional).
   */
  public View<Term> terms(Authorizations authorizations, String dataset, Set<String> fields, String term,
      BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return terms(batchScanner(compact(authorizations, dataset, null), NB_QUERY_THREADS), dataset, fields, term,
        bucketsIds);
  }

  /**
   * For each field of each bucket in a given dataset, get the number of occurrences of all terms in
   * {@code [minTerm, maxTerm]}. Note that this method only hits the forward index (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param minTerm        first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm        last searched term (excluded). Wildcard characters are not allowed.
   */
  public View<TermDistinctBuckets> termCardinalityEstimationForBuckets(Authorizations authorizations, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    BatchScanner scanner = batchScanner(compact(authorizations, dataset, null), NB_QUERY_THREADS);
    scanner.clearColumns();
    scanner.clearScanIterators();

    Key beginKey;
    Key endKey;

    if ((minTerm == null || minTerm instanceof String) && (maxTerm == null || maxTerm instanceof String)) {

      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + minTerm);
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + maxTerm);

      Preconditions.checkState(minTerm == null || !WildcardMatcher.hasWildcards((String) minTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(maxTerm == null || !WildcardMatcher.hasWildcards((String) maxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + StringCodec.defaultLexicoder(minTerm));
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + StringCodec.defaultLexicoder(maxTerm));
    }

    Range range = new Range(beginKey, endKey);
    return scanCounts(scanner, fields, range, false);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get buckets having a term in
   * {@code [minTerm, maxTerm]}. Note that this method only hits the forward index (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param minTerm        first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm        last searched term (excluded). Wildcard characters are not allowed.
   * @param bucketsIds     which buckets must be considered (optional).
   */
  public View<Term> termsSortedByTermAndBucketId(Authorizations authorizations, String dataset, Set<String> fields,
      Object minTerm, Object maxTerm, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return terms(scanner(compact(authorizations, dataset, null)), dataset, fields, minTerm, maxTerm, bucketsIds);
  }

  /**
   * For each field of a given list of buckets in a given dataset, get buckets having a term in
   * {@code [minTerm, maxTerm]}. Note that this method only hits the forward index (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset        dataset.
   * @param fields         which fields must be considered (optional).
   * @param minTerm        first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm        last searched term (excluded). Wildcard characters are not allowed.
   * @param bucketsIds     which buckets must be considered (optional).
   */
  public View<Term> terms(Authorizations authorizations, String dataset, Set<String> fields, Object minTerm,
      Object maxTerm, BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return terms(batchScanner(compact(authorizations, dataset, null), NB_QUERY_THREADS), dataset, fields, minTerm,
        maxTerm, bucketsIds);
  }

  private View<Term> terms(ScannerBase scanner, String dataset, Set<String> fields, String term,
      BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(!(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    scanner.clearColumns();
    scanner.clearScanIterators();

    boolean isTermBackward = WildcardMatcher.startsWithWildcard(term);
    String newTerm = isTermBackward ? new StringBuilder(term).reverse().toString() : term;

    Range range;

    if (!WildcardMatcher.hasWildcards(newTerm)) {
      range = Range.exact(dataset + SEPARATOR_NUL + newTerm, isTermBackward ? BACKWARD_INDEX : FORWARD_INDEX);
    } else {

      range = Range.prefix(dataset + SEPARATOR_NUL + WildcardMatcher.prefix(newTerm));

      IteratorSetting setting = new IteratorSetting(WILDCARD_FILTER_PRIORITY, "WildcardFilter1", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, dataset + SEPARATOR_NUL + newTerm);

      scanner.addScanIterator(setting);
    }
    return scanIndex(scanner, fields, range, isTermBackward, bucketsIds);
  }

  private View<Term> terms(ScannerBase scanner, String dataset, Set<String> fields, Object minTerm, Object maxTerm,
      BloomFilters<String> bucketsIds) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    scanner.clearColumns();
    scanner.clearScanIterators();

    Key beginKey;
    Key endKey;

    if ((minTerm == null || minTerm instanceof String) && (maxTerm == null || maxTerm instanceof String)) {

      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + minTerm);
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + maxTerm);

      Preconditions.checkState(minTerm == null || !WildcardMatcher.hasWildcards((String) minTerm),
          "wildcards are forbidden in minTerm");
      Preconditions.checkState(maxTerm == null || !WildcardMatcher.hasWildcards((String) maxTerm),
          "wildcards are forbidden in maxTerm");

    } else { // Objects other than String are lexicoded
      beginKey = minTerm == null ? null : new Key(dataset + SEPARATOR_NUL + StringCodec.defaultLexicoder(minTerm));
      endKey = maxTerm == null ? null : new Key(dataset + SEPARATOR_NUL + StringCodec.defaultLexicoder(maxTerm));
    }

    Range range = new Range(beginKey, endKey);
    return scanIndex(scanner, fields, range, false, bucketsIds);
  }
}
