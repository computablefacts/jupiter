package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.jupiter.storage.cache.Cache;
import com.computablefacts.jupiter.storage.termstore.FieldDistinctBuckets;
import com.computablefacts.jupiter.storage.termstore.FieldDistinctTerms;
import com.computablefacts.jupiter.storage.termstore.FieldLabels;
import com.computablefacts.jupiter.storage.termstore.FieldLastUpdate;
import com.computablefacts.jupiter.storage.termstore.FieldTopTerms;
import com.computablefacts.jupiter.storage.termstore.TermDistinctBuckets;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.StringIterator;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.computablefacts.nona.types.Span;
import com.computablefacts.nona.types.SpanSequence;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * This storage layer acts as a generic data store. For now, this storage layer is mostly used to
 * persist/retrieve JSON objects. Raw JSON objects are stored to a {@link BlobStore}. Indexed terms
 * are stored to a {@link TermStore}.
 * </p>
 *
 * <p>
 * Note that the {@link TermStore} also holds a hash index of all JSON values.
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
 *  <dataset>\0<hash>               | H               | <bucket_id>\0<field>              | (empty)                                     | (empty)
 * </pre>
 *
 * <p>
 * Another {@link BlobStore} holds cached data i.e. temporary computations.
 * </p>
 *
 * <pre>
 *  Row                          | Column Family | Column Qualifier                                | Visibility                             | Value
 * ==============================+===============+=================================================+========================================+========
 *  <dataset>\0<uuid>            | (empty)       | <cached_string>                                 | (empty)                                | (empty)
 *  <dataset>\0<uuid>            | (empty)       | <hashed_string>                                 | (empty)                                | <cached_string>
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class DataStore implements AutoCloseable {

  private static final Base64.Decoder b64Decoder_ = Base64.getDecoder();
  private static final Logger logger_ = LoggerFactory.getLogger(DataStore.class);

  private final String name_;
  private final BlobStore blobStore_;
  private final TermStore termStore_;
  private final Cache cache_;

  private AbstractBlobProcessor blobProcessor_;
  private AbstractTermProcessor termProcessor_;
  private AbstractHashProcessor hashProcessor_;

  public DataStore(Configurations configurations, String name) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ = new BlobStore(configurations, blobStoreName(name));
    termStore_ = new TermStore(configurations, termStoreName(name));
    cache_ = new Cache(configurations, cacheName(name));
    blobProcessor_ = newAccumuloBlobProcessor();
    termProcessor_ = newAccumuloTermProcessor();
    hashProcessor_ = newAccumuloHashProcessor();
  }

  @Beta
  public DataStore(Configurations configurations, String name, Authorizations authorizations) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ = new BlobStore(configurations, blobStoreName(name));
    termStore_ = new TermStore(configurations, termStoreName(name));
    cache_ = new Cache(configurations, cacheName(name));
    blobProcessor_ = newAccumuloBlobProcessor(authorizations, NB_QUERY_THREADS);
    termProcessor_ = newAccumuloTermProcessor(authorizations, NB_QUERY_THREADS);
    hashProcessor_ = newAccumuloHashProcessor(authorizations, NB_QUERY_THREADS);
  }

  static String normalize(String str) {
    return StringIterator.removeDiacriticalMarks(StringIterator.normalize(str)).toLowerCase();
  }

  @Generated
  public static String blobStoreName(String name) {
    return name + "Blobs";
  }

  @Generated
  public static String termStoreName(String name) {
    return name + "Terms";
  }

  @Generated
  public static String cacheName(String name) {
    return name + "Cache";
  }

  @Generated
  @Override
  public void close() {
    flush();
  }

  @Generated
  @Override
  protected void finalize() {
    flush();
  }

  @Beta
  public AccumuloBlobProcessor newAccumuloBlobProcessor() {
    return newAccumuloBlobProcessor(null, NB_QUERY_THREADS);
  }

  @Beta
  public AccumuloBlobProcessor newAccumuloBlobProcessor(Authorizations authorizations,
      int nbQueryThreads) {
    return new AccumuloBlobProcessor(blobStore_, authorizations, nbQueryThreads);
  }

  @Beta
  public AccumuloTermProcessor newAccumuloTermProcessor() {
    return newAccumuloTermProcessor(null, NB_QUERY_THREADS);
  }

  @Beta
  public AccumuloTermProcessor newAccumuloTermProcessor(Authorizations authorizations,
      int nbQueryThreads) {
    return new AccumuloTermProcessor(termStore_, authorizations, nbQueryThreads);
  }

  @Beta
  public AccumuloHashProcessor newAccumuloHashProcessor() {
    return newAccumuloHashProcessor(null, NB_QUERY_THREADS);
  }

  @Beta
  public AccumuloHashProcessor newAccumuloHashProcessor(Authorizations authorizations,
      int nbQueryThreads) {
    return new AccumuloHashProcessor(termStore_, authorizations, nbQueryThreads);
  }

  /**
   * Get a direct access to the underlying blob store.
   *
   * @return {@link BlobStore}
   */
  @Generated
  public BlobStore blobStore() {
    return blobStore_;
  }

  /**
   * Get a direct access to the underlying term store.
   *
   * @return {@link TermStore}
   */
  @Generated
  public TermStore termStore() {
    return termStore_;
  }

  /**
   * Get a direct access to the underlying cache.
   *
   * @return {@link Cache}
   */
  @Generated
  public Cache cache() {
    return cache_;
  }

  /**
   * Get the table configuration.
   *
   * @return the table configuration.
   */
  @Generated
  public Configurations configurations() {
    return blobStore_.configurations();
  }

  /**
   * Get the DataStore name.
   *
   * @return the DataStore name.
   */
  @Generated
  public String name() {
    return name_;
  }

  /**
   * Set the blob processor.
   *
   * @param blobProcessor the processor used to deal with terms when
   *        {@link #persistBlob(String, String, String)} is called.
   */
  @Generated
  public void setBlobProcessor(AbstractBlobProcessor blobProcessor) {
    if (blobProcessor_ != null) {
      try {
        blobProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    blobProcessor_ = blobProcessor;
  }

  /**
   * Set the term processor.
   *
   * @param termProcessor the processor used to deal with terms when
   *        {@link #persistTerm(String, String, String, Object, int)},
   *        {@link #docsIds(String, String, Set, BloomFilters)} or
   *        {@link #docsIds(String, Set, Object, Object, BloomFilters)} is called.
   */
  @Generated
  public void setTermProcessor(AbstractTermProcessor termProcessor) {
    if (termProcessor_ != null) {
      try {
        termProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    termProcessor_ = termProcessor;
  }

  /**
   * Set the hash processor.
   *
   * @param hashProcessor the processor used to deal with hashes when
   *        {@link #persistHash(String, String, String, Object)},
   *        {@link #matchHash(String, String, String)} or
   *        {@link #matchValue(String, String, Object)} is called.
   */
  @Generated
  public void setHashProcessor(AbstractHashProcessor hashProcessor) {
    if (hashProcessor_ != null) {
      try {
        hashProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    hashProcessor_ = hashProcessor;
  }

  public void flush() {
    if (blobProcessor_ != null) {
      try {
        blobProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    if (termProcessor_ != null) {
      try {
        termProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    if (hashProcessor_ != null) {
      try {
        hashProcessor_.close();
      } catch (Exception e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
  }

  public boolean grantWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  public boolean grantWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  public boolean grantWritePermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.grantPermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.READ);
  }

  public boolean revokeWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  public boolean revokeWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  public boolean revokeWritePermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return Users.revokePermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.READ);
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {
    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }
    return blobStore_.isReady() && termStore_.isReady() && cache_.isReady();
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  public boolean create() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).formatDebug());
    }

    @Var
    boolean isOk = blobStore_.create();
    isOk = isOk && termStore_.create();
    isOk = isOk && cache_.create();

    return isOk;
  }

  /**
   * Destroy the storage layer.
   *
   * @return true if the storage layer does not exist or has been successfully destroyed, false
   *         otherwise.
   */
  public boolean destroy() {
    return termStore_.destroy() && blobStore_.destroy() && cache_.destroy();
  }

  /**
   * Remove all data.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean truncate() {
    return termStore_.truncate() && blobStore_.truncate() && cache_.truncate();
  }

  /**
   * Remove all data for a given dataset.
   *
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean remove(String dataset) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("namespace", name()).add("dataset", dataset).formatDebug());
    }

    @Var
    boolean isOk = termStore_.removeDataset(dataset);
    isOk = isOk && blobStore_.removeDataset(dataset);
    isOk = isOk && cache_.removeDataset(dataset);

    return isOk;
  }

  /**
   * This method should be called once, at the end of the ingest process.
   *
   * If the {@link #beginIngest()} and {@link #endIngest(String)} methods are called too often, the
   * estimators may be heavily skewed towards a subset of the data.
   */
  public void beginIngest() {
    termStore_.beginIngest();
  }

  /**
   * This method should be called once, at the end of the ingest process.
   *
   * If the {@link #beginIngest()} and {@link #endIngest(String)} methods are called too often, the
   * estimators may be heavily skewed towards a subset of the data.
   *
   * @param dataset the dataset.
   * @return true if the write operations succeeded, false otherwise.
   */
  @CanIgnoreReturnValue
  public boolean endIngest(String dataset) {
    return termProcessor_ == null || (termProcessor_ instanceof AccumuloTermProcessor
        && termStore_.endIngest(((AccumuloTermProcessor) termProcessor_).writer(), dataset));
  }

  /**
   * Persist a single JSON object.
   *
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(String dataset, String docId, String json) {
    return persistJson(dataset, docId, json, key -> true, Codecs.defaultTokenizer, true);
  }

  /**
   * Persist a single JSON object.
   *
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(String dataset, String docId, Map<String, Object> json) {
    return persistJson(dataset, docId, Codecs.asString(json), key -> true, Codecs.defaultTokenizer,
        true);
  }

  /**
   * Reindex a single JSON object.
   *
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean reindex(String dataset, String docId, String json) {
    return persistJson(dataset, docId, json, key -> true, Codecs.defaultTokenizer, false);
  }

  /**
   * Reindex a single JSON object.
   *
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean reindex(String dataset, String docId, Map<String, Object> json) {
    return persistJson(dataset, docId, Codecs.asString(json), key -> true, Codecs.defaultTokenizer,
        false);
  }

  /**
   * Get the visibility labels available for a given field.
   *
   * @param dataset dataset.
   * @param field field.
   * @return visibility labels. No particular order should be expected from the returned iterator.
   */
  public Iterator<FieldLabels> fieldVisibilityLabels(String dataset, String field) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<FieldLabels> fl = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {
      termStore_
          .fieldVisibilityLabels(scanner, dataset, field == null ? null : Sets.newHashSet(field))
          .forEachRemaining(fl::add);
    }
    return fl.iterator();
  }

  /**
   * Get the date of last of a given field.
   *
   * @param dataset dataset.
   * @param field field.
   * @return last update as an UTC timestamp. No particular order should be expected from the
   *         returned iterator.
   */
  public Iterator<FieldLastUpdate> fieldLastUpdate(String dataset, String field) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<FieldLastUpdate> flu = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {
      termStore_.fieldLastUpdate(scanner, dataset, field == null ? null : Sets.newHashSet(field))
          .forEachRemaining(flu::add);
    }
    return flu.iterator();
  }

  /**
   * Get the number of distinct terms for a given field.
   *
   * @param dataset dataset.
   * @param field field.
   * @return cardinality estimation. No particular order should be expected from the returned
   *         iterator.
   */
  public Iterator<FieldDistinctTerms> fieldCardinalityEstimationForTerms(String dataset,
      String field) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<FieldDistinctTerms> fdt = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {
      termStore_.fieldCardinalityEstimationForTerms(scanner, dataset,
          field == null ? null : Sets.newHashSet(field)).forEachRemaining(fdt::add);
    }
    return fdt.iterator();
  }

  /**
   * Get the number of distinct buckets for a given field.
   *
   * @param dataset dataset.
   * @param field field.
   * @return cardinality estimation. No particular order should be expected from the returned
   *         iterator.
   */
  public Iterator<FieldDistinctBuckets> fieldCardinalityEstimationForBuckets(String dataset,
      String field) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<FieldDistinctBuckets> fdb = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {
      termStore_.fieldCardinalityEstimationForBuckets(scanner, dataset,
          field == null ? null : Sets.newHashSet(field)).forEachRemaining(fdb::add);
    }
    return fdb.iterator();
  }

  /**
   * Get the number of distinct buckets for a given field.
   *
   * @param dataset dataset.
   * @param field field.
   * @return top terms. No particular order should be expected from the returned iterator.
   */
  public Iterator<FieldTopTerms> fieldTopTerms(String dataset, String field) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<FieldTopTerms> ftt = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {
      termStore_.fieldTopTerms(scanner, dataset, field == null ? null : Sets.newHashSet(field))
          .forEachRemaining(ftt::add);
    }
    return ftt.iterator();
  }

  /**
   * Get all JSON from the blob storage layer. Note that using a BatchScanner improves performances
   * a lot.
   *
   * The <dataset>_RAW_DATA auth is not enough to get access to the full JSON document. The user
   * must also have the <dataset>_<field> auth for each requested field.
   *
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public Iterator<Blob<Value>> jsonScan(String dataset, Set<String> fields) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (blobProcessor_ == null || !(blobProcessor_ instanceof AccumuloBlobProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<Blob<Value>> blobs = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloBlobProcessor) blobProcessor_).scanner()) {
      blobStore_.getJsons(scanner, dataset, null, fields).forEachRemaining(blobs::add);
    }
    return blobs.iterator();
  }

  /**
   * Get JSON from the blob storage layer.
   *
   * The <dataset>_RAW_DATA auth is not enough to get access to the full JSON document. The user
   * must also have the <dataset>_<field> auth for each requested field.
   *
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @param docsIds documents unique identifiers.
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public Iterator<Blob<Value>> jsonScan(String dataset, Set<String> fields, Set<String> docsIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docsIds, "docsIds should not be null");


    if (blobProcessor_ == null || !(blobProcessor_ instanceof AccumuloBlobProcessor)) {
      return ITERATOR_EMPTY;
    }

    // TODO : load docs on-demand to prevent segfault
    Set<Blob<Value>> blobs = new HashSet<>();

    try (ScannerBase scanner = ((AccumuloBlobProcessor) blobProcessor_).scanner()) {
      blobStore_.getJsons(scanner, dataset, docsIds, fields).forEachRemaining(blobs::add);
    }
    return blobs.iterator();
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of a given term.
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return the estimated number of occurrences of the given term.
   */
  public long termCardinalityEstimationForBuckets(String dataset, Set<String> fields, String term) {

    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return 0;
    }

    @Var
    long count = 0;

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {

      Iterator<TermDistinctBuckets> iter =
          termStore_.termCardinalityEstimationForBuckets(scanner, dataset, fields, term);

      while (iter.hasNext()) {
        TermDistinctBuckets termCount = iter.next();
        count += termCount.count();
      }
    }
    return count;
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of all terms in [minTerm,
   * maxTerm].
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @return the estimated number of terms in [minTerm, maxTerm].
   */
  public long termCardinalityEstimationForBuckets(String dataset, Set<String> fields,
      Object minTerm, Object maxTerm) {

    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    if (termProcessor_ == null || !(termProcessor_ instanceof AccumuloTermProcessor)) {
      return 0;
    }

    @Var
    long count = 0;

    try (ScannerBase scanner = ((AccumuloTermProcessor) termProcessor_).scanner()) {

      Iterator<TermDistinctBuckets> iter = termStore_.termCardinalityEstimationForBuckets(scanner,
          dataset, fields, minTerm, maxTerm);

      while (iter.hasNext()) {
        TermDistinctBuckets termCount = iter.next();
        count += termCount.count();
      }
    }
    return count;
  }

  /**
   * Get the ids of all documents where at least one token matches "term".
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public Iterator<String> docsIds(String dataset, String term, Set<String> fields,
      BloomFilters<String> docsIds) {
    return termProcessor_ == null ? ITERATOR_EMPTY
        : termProcessor_.read(dataset, term, fields, docsIds);
  }

  /**
   * Get the ids of all documents where at least one token matches a term in [minTerm, maxTerm].
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public Iterator<String> docsIds(String dataset, Set<String> fields, Object minTerm,
      Object maxTerm, BloomFilters<String> docsIds) {
    return termProcessor_ == null ? ITERATOR_EMPTY
        : termProcessor_.read(dataset, fields, minTerm, maxTerm, docsIds);
  }

  /**
   * Get the ids of all documents where a field value exactly matches a given value.
   *
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param value the value to match.
   * @return an unordered stream of documents ids.
   */
  public Iterator<String> matchValue(String dataset, String field, Object value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("field", field).add("value", value).add("has_hash_processor", hashProcessor_ != null)
          .formatDebug());
    }
    return hashProcessor_ == null ? ITERATOR_EMPTY
        : hashProcessor_.read(dataset, field, MaskingIterator.hash(null, value.toString()));
  }

  /**
   * Get the ids of all documents where a field hashed value exactly matches a given hash.
   *
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param hash the hash to match.
   * @return an unordered stream of documents ids.
   */
  public Iterator<String> matchHash(String dataset, String field, String hash) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(hash, "hash should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("field", field).add("hash", hash).add("has_hash_processor", hashProcessor_ != null)
          .formatDebug());
    }
    return hashProcessor_ == null ? ITERATOR_EMPTY : hashProcessor_.read(dataset, field, hash);
  }

  /**
   * Return misc. infos about a given list of datasets.
   *
   * @param datasets a list of datasets.
   * @return {@link DataStoreInfos}.
   */
  public DataStoreInfos infos(Set<String> datasets) {

    DataStoreInfos infos = new DataStoreInfos(name());

    datasets.forEach(dataset -> {

      Iterator<FieldDistinctTerms> cardEstForTermsIterator =
          fieldCardinalityEstimationForTerms(dataset, null);

      while (cardEstForTermsIterator.hasNext()) {
        FieldDistinctTerms distinctTerms = cardEstForTermsIterator.next();
        infos.addCardinalityEstimationForTerms(dataset, distinctTerms.field(), distinctTerms.type(),
            distinctTerms.estimate());
      }

      Iterator<FieldDistinctBuckets> cardEstForBucketsIterator =
          fieldCardinalityEstimationForBuckets(dataset, null);

      while (cardEstForBucketsIterator.hasNext()) {
        FieldDistinctBuckets distinctBuckets = cardEstForBucketsIterator.next();
        infos.addCardinalityEstimationForBuckets(dataset, distinctBuckets.field(),
            distinctBuckets.type(), distinctBuckets.estimate());
      }

      Iterator<FieldTopTerms> topTermsIterator = fieldTopTerms(dataset, null);

      while (topTermsIterator.hasNext()) {
        FieldTopTerms topTerms = topTermsIterator.next();
        infos.addTopTermsNoFalsePositives(dataset, topTerms.field(), topTerms.type(),
            topTerms.topTermsNoFalsePositives());
        infos.addTopTermsNoFalseNegatives(dataset, topTerms.field(), topTerms.type(),
            topTerms.topTermsNoFalseNegatives());
      }

      Iterator<FieldLabels> labelsIterator = fieldVisibilityLabels(dataset, null);

      while (labelsIterator.hasNext()) {
        FieldLabels labels = labelsIterator.next();
        infos.addVisibilityLabels(dataset, labels.field(), labels.type(), labels.termLabels());
      }

      Iterator<FieldLastUpdate> lastUpdateIterator = fieldLastUpdate(dataset, null);

      while (lastUpdateIterator.hasNext()) {
        FieldLastUpdate lastUpdate = lastUpdateIterator.next();
        infos.addLastUpdate(dataset, lastUpdate.field(), lastUpdate.type(),
            lastUpdate.lastUpdate());
      }
    });
    return infos;
  }

  /**
   * Persist a single JSON object.
   *
   * @param dataset the dataset.
   * @param docId the document identifier
   * @param json the JSON object as a String.
   * @param keepField filter applied on all JSON attributes before value tokenization (optional).
   *        This predicate should return true iif the field's value must be indexed.
   * @param tokenizer string tokenizer (optional).
   * @param jsonAsBlob the json will be persisted as a blob iif this parameter is set to true.
   * @return true if the operation succeeded, false otherwise.
   */
  private boolean persistJson(String dataset, String docId, String json,
      Predicate<String> keepField, Function<String, SpanSequence> tokenizer, boolean jsonAsBlob) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(json, "json should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("docId", docId).add("json", json).add("has_keep_field", keepField != null)
          .add("has_tokenizer", tokenizer != null).add("json_as_blob", jsonAsBlob).formatDebug());
    }

    if (jsonAsBlob) {
      if (!persistBlob(dataset, docId, json)) {
        return false;
      }
    }

    Map<String, Multiset<Object>> fields = new HashMap<>();
    Map<String, Object> newJson =
        new JsonFlattener(json).withSeparator(SEPARATOR_CURRENCY_SIGN).flattenAsMap();

    for (String field : newJson.keySet()) {

      // Attributes starting with an underscore should not be indexed
      if (field.startsWith("_") || field.contains(SEPARATOR_CURRENCY_SIGN + "_")) {
        continue;
      }
      if (keepField != null && !keepField.test(field)) {
        continue;
      }

      // Serialize all values to strings and tokenize these strings to a span sequence
      @Var
      Object value = newJson.get(field);

      if (value == null) { // Ignore keys with null values
        continue;
      }

      // Because the CSV file format does not have types, check if a string value can be directly
      // mapped to a primitive in {boolean, integer, decimal, date}
      if (value instanceof String) {
        String text = (String) value;
        if ("true".equalsIgnoreCase(text)) {
          value = true;
        } else if ("false".equalsIgnoreCase(text)) {
          value = false;
        } else if (!text.contains("E") && !text.contains("e")
        /* ensure 79E2863560 is not coerced to 7.9E+2863561 */
            && com.computablefacts.nona.helpers.Strings.isNumber(text)) {
          try {

            value = new BigInteger(text);

            // Here, text is an integer (otherwise a NumberFormatException has been thrown)
            StringIterator iterator = new StringIterator(text);
            iterator.movePast(new char[] {'0'});

            // The condition below ensures "0" is interpreted as a number but "00" as a string
            if (iterator.position() > 1 || (iterator.position() > 0 && iterator.remaining() > 0)) {

              // Ensure 00 is not mapped to 0
              // Ensure 007 is not mapped to 7
              value = text;
            }
          } catch (NumberFormatException nfe1) {
            try {

              value = new BigDecimal(text);

              // Here, text is a decimal number (otherwise a NumberFormatException has been thrown)
              if (text.trim().endsWith(".") || text.trim().startsWith(".")) {

                // Ensure 123. is not mapped to 123.0
                // Ensure .123 is not mapped to 0.123
                value = text;
              }
            } catch (NumberFormatException nfe2) {
              value = text;
            }
          }
        } else {

          // Because the JSON file format does not have a date type, check if val is in ISO Instant
          // format
          if (text.length() >= 20 && text.length() <= 24
              && (text.charAt(10) == 'T' || text.charAt(10) == 't')
              && (text.charAt(text.length() - 1) == 'Z' || text.charAt(text.length() - 1) == 'z')) {
            try {
              value = Date.from(Instant.parse(text));
            } catch (Exception e) {
              value = text;
            }
          }
        }
      }

      String newField = field.replaceAll("\\[\\d+\\]", "[*]");

      if (!fields.containsKey(newField)) {
        fields.put(newField, HashMultiset.create());
      }

      if (!persistHash(dataset, docId, newField, value)) {
        logger_
            .error(LogFormatter.create(true)
                .message(String.format(
                    "Persistence of the hash for field %s failed for document %s.", field, docId))
                .formatError());
        return false;
      }

      if (!(value instanceof String)) {
        fields.get(newField).add(value); // Objects other than String will be lexicoded by the
                                         // TermStore
      } else {

        String val = ((String) value).trim();

        if (Codecs.isProbablyBase64(val)) {
          try {
            Object newVal = Codecs.decodeB64(b64Decoder_, val);
            continue; // Base64 strings are NOT indexed
          } catch (Exception e) {
            // FALL THROUGH
          }
        }

        SpanSequence spanSequence;

        if (tokenizer != null) {
          spanSequence = Objects.requireNonNull(tokenizer.apply(val));
        } else {
          String str = normalize(value.toString());
          spanSequence = new SpanSequence();
          spanSequence.add(new Span(str, 0, str.length()));
        }

        spanSequence.forEach(span -> fields.get(newField).add(span.text()));
      }
    }

    newJson.clear(); // free up memory

    // Persist terms
    @Var
    boolean isOk = true;

    for (Map.Entry<String, Multiset<Object>> field : fields.entrySet()) {
      for (Multiset.Entry<Object> term : field.getValue().entrySet()) {
        isOk =
            isOk && persistTerm(dataset, docId, field.getKey(), term.getElement(), term.getCount());
      }
    }
    return isOk;
  }

  private boolean persistBlob(String dataset, String docId, String blob) {
    return blobProcessor_ == null || blobProcessor_.write(dataset, docId, blob);
  }

  private boolean persistTerm(String dataset, String docId, String field, Object term,
      int nbOccurrencesInDoc) {
    return termProcessor_ == null
        || termProcessor_.write(dataset, docId, field, term, nbOccurrencesInDoc);
  }

  private boolean persistHash(String dataset, String docId, String field, Object value) {
    return hashProcessor_ == null || hashProcessor_.write(dataset, docId, field, value);
  }
}
