package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.*;
import static com.computablefacts.jupiter.storage.datastore.AccumuloHashProcessor.CF;

import java.util.*;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.asterix.*;
import com.computablefacts.asterix.codecs.Base64Codec;
import com.computablefacts.asterix.codecs.JsonCodec;
import com.computablefacts.asterix.codecs.StringCodec;
import com.computablefacts.jupiter.*;
import com.computablefacts.jupiter.filters.TermStoreBucketFieldFilter;
import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.jupiter.storage.cache.Cache;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
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

  private final AbstractBlobProcessor blobProcessor_;
  private final AbstractTermProcessor termProcessor_;
  private final AbstractHashProcessor hashProcessor_;

  public DataStore(Configurations configurations, String name) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ = new BlobStore(configurations, blobStoreName(name));
    termStore_ = new TermStore(configurations, termStoreName(name));
    cache_ = new Cache(configurations, cacheName(name));
    blobProcessor_ = new AccumuloBlobProcessor(blobStore_);
    termProcessor_ = new AccumuloTermProcessor(termStore_);
    hashProcessor_ = new AccumuloHashProcessor(termStore_);
  }

  @Beta
  public DataStore(String name, BlobStore blobStore, TermStore termStore, Cache cache,
      AbstractBlobProcessor blobProcessor, AbstractTermProcessor termProcessor,
      AbstractHashProcessor hashProcessor) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ =
        Preconditions.checkNotNull(blobStore, "blobStore should neither be null nor empty");
    termStore_ =
        Preconditions.checkNotNull(termStore, "termStore should neither be null nor empty");
    cache_ = Preconditions.checkNotNull(cache, "cache should neither be null nor empty");
    blobProcessor_ = blobProcessor;
    termProcessor_ = termProcessor;
    hashProcessor_ = hashProcessor;
  }

  static String normalize(String str) {
    return StringCodec.removeDiacriticalMarks(StringCodec.normalize(str)).toLowerCase();
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

    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  public boolean grantWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  public boolean grantWritePermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.WRITE);
  }

  public boolean grantReadPermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.READ);
  }

  public boolean revokeWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  public boolean revokeWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  public boolean revokeWritePermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.WRITE);
  }

  public boolean revokeReadPermissionOnCache(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(cache_.configurations().connector(), username, cacheName(name()),
        TablePermission.READ);
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {
    return blobStore_.isReady() && termStore_.isReady() && cache_.isReady();
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  public boolean create() {

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

    @Var
    boolean isOk = termStore_.removeDataset(dataset);
    isOk = isOk && blobStore_.removeDataset(dataset);
    isOk = isOk && cache_.removeDataset(dataset);

    return isOk;
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
    return persistJson(dataset, docId, json, key -> true, StringCodec::defaultTokenizer, true);
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
    return persistJson(dataset, docId, JsonCodec.asString(json), key -> true,
        StringCodec::defaultTokenizer, true);
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
    return persistJson(dataset, docId, json, key -> true, StringCodec::defaultTokenizer, false);
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
    return persistJson(dataset, docId, JsonCodec.asString(json), key -> true,
        StringCodec::defaultTokenizer, false);
  }

  /**
   * Get all JSON from the blob storage layer (sorted).
   *
   * The {@code <dataset>_RAW_DATA} auth is not enough to get access to the full JSON document. The
   * user must also have the {@code <dataset>_<field>} auth for each requested field.
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public View<Blob<Value>> jsonsSortedByKey(Authorizations authorizations, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return blobStore_.jsonsSortedByKey(authorizations, dataset, null, fields);
  }

  /**
   * Get all JSON from the blob storage layer (unsorted).
   *
   * The {@code <dataset>_RAW_DATA} auth is not enough to get access to the full JSON document. The
   * user must also have the {@code <dataset>_<field>} auth for each requested field.
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public View<Blob<Value>> jsons(Authorizations authorizations, String dataset,
      Set<String> fields) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return blobStore_.jsons(authorizations, dataset, null, fields);
  }

  /**
   * Get JSON from the blob storage layer (sorted).
   *
   * The {@code <dataset>_RAW_DATA} auth is not enough to get access to the full JSON document. The
   * user must also have the {@code <dataset>_<field>} auth for each requested field.
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @param docsIds documents unique identifiers.
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public View<Blob<Value>> jsonsSortedByKey(Authorizations authorizations, String dataset,
      Set<String> fields, Set<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docsIds, "docsIds should not be null");

    return blobStore_.jsonsSortedByKey(authorizations, dataset, docsIds, fields);
  }

  /**
   * Get JSON from the blob storage layer (unsorted).
   *
   * The {@code <dataset>_RAW_DATA} auth is not enough to get access to the full JSON document. The
   * user must also have the {@code <dataset>_<field>} auth for each requested field.
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @param docsIds documents unique identifiers.
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public View<Blob<Value>> jsons(Authorizations authorizations, String dataset, Set<String> fields,
      Set<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docsIds, "docsIds should not be null");

    return blobStore_.jsons(authorizations, dataset, docsIds, fields);
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of a given {@code term}.
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return the estimated number of occurrences of the given term.
   */
  public long termCardinalityEstimationForBuckets(Authorizations authorizations, String dataset,
      Set<String> fields, String term) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    return termStore_.termCardinalityEstimationForBuckets(authorizations, dataset, fields, term)
        .reduce(0L, (carry, t) -> carry + t.count());
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of all terms in {@code [minTerm,
   * maxTerm]}.
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @return the estimated number of terms in [minTerm, maxTerm].
   */
  public long termCardinalityEstimationForBuckets(Authorizations authorizations, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    return termStore_
        .termCardinalityEstimationForBuckets(authorizations, dataset, fields, minTerm, maxTerm)
        .reduce(0L, (carry, t) -> carry + t.count());
  }

  /**
   * Get the ids of all documents where at least one token matches {@code term} (ordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public View<String> docsIdsSorted(Authorizations authorizations, String dataset, String term,
      Set<String> fields, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    View<String> CARRY = View.of();
    return termsSortedByTermAndDocId(authorizations, dataset, term, fields, docsIds)
        .groupSorted((t1, t2) -> t1.term().equals(t2.term())).reduce(CARRY, (carry, termz) -> {

          // On the first iteration, extract docs ids (sorted) and returns the whole list
          if (carry == CARRY) {
            return termz.map(Term::bucketId);
          }

          // On subsequent iterations, extract docs ids (sorted) and merge the previous list
          // with the current one
          List<View<String>> list = new ArrayList<>();
          list.add(carry);

          return termz.map(Term::bucketId).mergeSorted(list, String::compareTo).dedupSorted();
        });
  }

  public View<Term> termsSortedByTermAndDocId(Authorizations authorizations, String dataset,
      String term, Set<String> fields, BloomFilters<String> docsIds) {
    return termStore_.termsSortedByTermAndBucketId(authorizations, dataset, fields, term, docsIds);
  }

  /**
   * Get the ids of all documents where at least one token matches a term in
   * {@code [minTerm, maxTerm]} (ordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public View<String> docsIdsSorted(Authorizations authorizations, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    View<String> CARRY = View.of();
    return termsSortedByTermAndDocId(authorizations, dataset, fields, minTerm, maxTerm, docsIds)
        .groupSorted((t1, t2) -> t1.term().equals(t2.term())).reduce(CARRY, (carry, termz) -> {

          // On the first iteration, extract docs ids (sorted) and returns the whole list
          if (carry == CARRY) {
            return termz.map(Term::bucketId);
          }

          // On subsequent iterations, extract docs ids (sorted) and merge the previous list
          // with the current one
          List<View<String>> list = new ArrayList<>();
          list.add(carry);

          return termz.map(Term::bucketId).mergeSorted(list, String::compareTo).dedupSorted();
        });
  }

  public View<Term> termsSortedByTermAndDocId(Authorizations authorizations, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm, BloomFilters<String> docsIds) {
    return termStore_.termsSortedByTermAndBucketId(authorizations, dataset, fields, minTerm,
        maxTerm, docsIds);
  }

  /**
   * Get the ids of all documents where at least one token matches {@code term} (unordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param docsIds which docs must be considered (optional).
   * @return an unordered stream of terms.
   */
  public View<Term> terms(Authorizations authorizations, String dataset, String term,
      Set<String> fields, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    return termStore_.terms(authorizations, dataset, fields, term, docsIds);
  }

  /**
   * Get the ids of all documents where at least one token matches a term in
   * {@code [minTerm, maxTerm]} (unordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param docsIds which docs must be considered (optional).
   * @return an unordered stream of terms.
   */
  public View<Term> terms(Authorizations authorizations, String dataset, Set<String> fields,
      Object minTerm, Object maxTerm, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");
    return termStore_.terms(authorizations, dataset, fields, minTerm, maxTerm, docsIds);
  }

  /**
   * Get the ids of all documents where a field value exactly matches a given value (ordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param value the value to match.
   * @return an ordered stream of documents ids.
   */
  public View<String> matchValueSortedByDocId(Authorizations authorizations, String dataset,
      String field, Object value) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return readHash(termStore_.scanner(authorizations), dataset, field,
        MaskingIterator.hash(null, value.toString()));
  }

  /**
   * Get the ids of all documents where a field value exactly matches a given value (unordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param value the value to match.
   * @return an unordered stream of documents ids.
   */
  public View<String> matchValue(Authorizations authorizations, String dataset, String field,
      Object value) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return readHash(termStore_.batchScanner(authorizations, NB_QUERY_THREADS), dataset, field,
        MaskingIterator.hash(null, value.toString()));
  }

  /**
   * Get the ids of all documents where a field hashed value exactly matches a given hash (ordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param hash the hash to match.
   * @return an ordered stream of documents ids.
   */
  public View<String> matchHashSortedByDocId(Authorizations authorizations, String dataset,
      String field, String hash) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(hash, "hash should not be null");

    return readHash(termStore_.scanner(authorizations), dataset, field, hash);
  }

  /**
   * Get the ids of all documents where a field hashed value exactly matches a given hash
   * (unordered).
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @param field which field must be considered.
   * @param hash the hash to match.
   * @return an unordered stream of documents ids.
   */
  public View<String> matchHash(Authorizations authorizations, String dataset, String field,
      String hash) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(hash, "hash should not be null");

    return readHash(termStore_.batchScanner(authorizations, NB_QUERY_THREADS), dataset, field,
        hash);
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
        value = StringCodec.defaultCoercer(value, false);
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

        if (Base64Codec.isProbablyBase64(val)) {
          try {
            Object newVal = Base64Codec.decodeB64(b64Decoder_, val);
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

  private View<String> readHash(ScannerBase scanner, String dataset, String field, String hash) {

    Preconditions.checkNotNull(scanner, "scanner should neither be null nor empty");
    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(CF));

    Range range;

    if (hash != null) {
      range = Range.exact(dataset + SEPARATOR_NUL + hash, CF);
    } else {
      range = Range.prefix(dataset + SEPARATOR_NUL);
    }

    if (!AbstractStorage.setRanges(scanner, Sets.newHashSet(range))) {
      return View.of();
    }

    if (field != null) {

      IteratorSetting setting =
          new IteratorSetting(31, "TermStoreBucketFieldFilter", TermStoreBucketFieldFilter.class);
      TermStoreBucketFieldFilter.setFieldsToKeep(setting, Sets.newHashSet(field));

      scanner.addScanIterator(setting);
    }

    View<Map.Entry<Key, Value>> view;

    if (scanner instanceof BatchScanner) {
      view = new UnorderedView<>((BatchScanner) scanner, s -> s.iterator());
    } else {
      view = new OrderedView<>((Scanner) scanner, s -> s.iterator());
    }
    return view.map(e -> {
      String cq = e.getKey().getColumnQualifier().toString();
      return cq.substring(0, cq.indexOf(SEPARATOR_NUL));
    });
  }
}
