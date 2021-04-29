package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.STRING_RAW_DATA;
import static com.computablefacts.jupiter.storage.Constants.TEXT_CACHE;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.filters.AgeOffPeriodFilter;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
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
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class DataStore {

  private static final HashFunction hashFunction_ = Hashing.murmur3_128();
  private static final Base64.Decoder b64Decoder_ = Base64.getDecoder();
  private static final Logger logger_ = LoggerFactory.getLogger(DataStore.class);

  private final String name_;
  private final BlobStore blobStore_;
  private final TermStore termStore_;

  public DataStore(Configurations configurations, String name) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ = new BlobStore(configurations, blobStoreName(name));
    termStore_ = new TermStore(configurations, termStoreName(name));
  }

  static String normalize(String str) {
    return StringIterator.removeDiacriticalMarks(StringIterator.normalize(str)).toLowerCase();
  }

  @Generated
  static String blobStoreName(String name) {
    return name + "Blobs";
  }

  @Generated
  static String termStoreName(String name) {
    return name + "Terms";
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
   * Get scanners.
   *
   * @param authorizations authorizations.
   * @return scanners.
   */
  @Deprecated
  public Scanners scanners(Authorizations authorizations) {
    return new Scanners(configurations(), name(), authorizations);
  }

  /**
   * Get writers.
   *
   * @return writers.
   */
  @Deprecated
  public Writers writers() {
    return new Writers(configurations(), name());
  }

  @Deprecated
  public boolean grantWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean grantReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean grantWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean grantReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean revokeWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean revokeReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean revokeWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean revokeReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  /**
   * Grant the READ permission on the underlying tables for a given user.
   *
   * @param username user.
   * @return tru iif the READ permission has been granted, false otherwise.
   */
  public boolean grantReadPermissions(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return grantReadPermissionOnBlobStore(username) && grantReadPermissionOnTermStore(username);
  }

  /**
   * Revoke the READ permission on the underlying tables for a given user.
   *
   * @param username user.
   * @return tru iif the READ permission has been revoked, false otherwise.
   */
  public boolean revokeReadPermissions(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return revokeReadPermissionOnBlobStore(username) && revokeReadPermissionOnTermStore(username);
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {
    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }
    return blobStore_.isReady() && termStore_.isReady();
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  public boolean create() {

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("namespace", name()).formatInfo());
    }

    boolean isReady = blobStore_.isReady() && termStore_.isReady();

    if (!isReady && blobStore_.create() && termStore_.create()) {
      try {

        // Set a 3 hours TTL on all cached data
        IteratorSetting settings = new IteratorSetting(7, AgeOffPeriodFilter.class);
        AgeOffPeriodFilter.setColumnFamily(settings, TEXT_CACHE.toString());
        AgeOffPeriodFilter.setTtl(settings, 3);
        AgeOffPeriodFilter.setTtlUnits(settings, "HOURS");

        configurations().tableOperations().attachIterator(blobStore_.tableName(), settings);

      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return true;
  }

  /**
   * Destroy the storage layer.
   *
   * @return true if the storage layer does not exist or has been successfully destroyed, false
   *         otherwise.
   */
  public boolean destroy() {
    return termStore_.destroy() && blobStore_.destroy();
  }

  /**
   * Remove all data.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean truncate() {
    return termStore_.truncate() && blobStore_.truncate();
  }

  /**
   * Remove all data for a given dataset. The caller MUST HAVE the ADM visibility label for this
   * call to succeed.
   *
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean remove(String dataset) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(
          LogFormatter.create(true).add("namespace", name()).add("dataset", dataset).formatInfo());
    }

    @Var
    boolean isOk = true;
    Authorizations auths = new Authorizations(STRING_ADM);

    try (BatchDeleter deleter = termStore_.deleter(auths)) {
      isOk = isOk && termStore_.removeDataset(deleter, dataset);
    }
    try (BatchDeleter deleter = blobStore_.deleter(auths)) {
      isOk = isOk && blobStore_.removeDataset(deleter, dataset);
    }
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
      logger_.info(
          LogFormatter.create(true).add("namespace", name()).add("dataset", dataset).formatInfo());
    }

    boolean isOk1 = blobStore_.addLocalityGroup(dataset);
    boolean isOk2 = termStore_.addLocalityGroup(dataset);

    return isOk1 && isOk2;
  }

  @Beta
  public void beginIngest() {
    termStore_.beginIngest();
  }

  @Beta
  @CanIgnoreReturnValue
  public boolean endIngest(Writers writers, String dataset) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.endIngest(writers.index(), dataset);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, String dataset, String docId, String json) {
    return persist(writers, dataset, docId, json, key -> true, Codecs.defaultTokenizer);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param dataset dataset.
   * @param docId unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, String dataset, String docId, Map<String, Object> json) {
    return persist(writers, dataset, docId, Codecs.asString(json), key -> true,
        Codecs.defaultTokenizer);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param dataset the dataset.
   * @param docId the document identifier
   * @param json the JSON object as a String.
   * @param keepField filter applied on all JSON attributes before value tokenization (optional).
   *        This predicate should return true iif the field's value must be indexed.
   * @param tokenizer string tokenizer (optional).
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, String dataset, String docId, String json,
      Predicate<String> keepField, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(json, "json should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("docId", docId).add("json", json).add("has_keep_field", keepField != null)
          .add("has_tokenizer", tokenizer != null).formatDebug());
    }

    if (!persistBlob(writers, dataset, docId, json)) {
      return false;
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
      Object value = newJson.get(field);

      if (value == null) { // Ignore keys with null values
        continue;
      }

      String newField = field.replaceAll("\\[\\d+\\]", "[*]");

      if (!fields.containsKey(newField)) {
        fields.put(newField, HashMultiset.create());
      }

      if (!(value instanceof String)) {
        fields.get(newField).add(value); // Objects other than String will be lexicoded by the
                                         // TermStore
      } else {

        String val = ((String) value).trim();
        @Var
        Object newVal = val;

        if (Codecs.isProbablyBase64(val)) {
          try {
            newVal = Codecs.decodeB64(b64Decoder_, val);
            continue; // Base64 strings are NOT indexed
          } catch (Exception e) {
            // fall through
          }
        }

        // Because JSON does not have a date format, check if val is in ISO Instant format
        if (val.length() >= 20 && val.length() <= 24
            && (val.charAt(10) == 'T' || val.charAt(10) == 't')
            && (val.charAt(val.length() - 1) == 'Z' || val.charAt(val.length() - 1) == 'z')) {
          try {
            newVal = Date.from(Instant.parse(val));
          } catch (Exception e) {
            // fall through
          }
        }

        if (!(newVal instanceof String)) {
          fields.get(newField).add(newVal);
        } else {

          SpanSequence spanSequence;

          if (tokenizer != null) {
            spanSequence = Objects.requireNonNull(tokenizer.apply((String) newVal));
          } else {
            String str = normalize(value.toString());
            spanSequence = new SpanSequence();
            spanSequence.add(new Span(str, 0, str.length()));
          }

          spanSequence.forEach(span -> fields.get(newField).add(span.text()));
        }
      }
    }

    newJson.clear(); // free up memory

    // Persist terms
    @Var
    boolean isOk = true;

    for (Map.Entry<String, Multiset<Object>> field : fields.entrySet()) {
      for (Multiset.Entry<Object> term : field.getValue().entrySet()) {
        isOk =
            persistTerm(writers, dataset, docId, field.getKey(), term.getElement(), term.getCount())
                && isOk;
      }
    }
    return isOk;
  }

  /**
   * Get the visibility labels available for a given field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return visibility labels. No particular order should be expected from the returned iterator.
   */
  public Iterator<FieldLabels> fieldVisibilityLabels(Scanners scanners, String dataset,
      String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldVisibilityLabels(scanners.index(NB_QUERY_THREADS), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get the date of last of a given field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return last update as an UTC timestamp. No particular order should be expected from the
   *         returned iterator.
   */
  public Iterator<FieldLastUpdate> fieldLastUpdate(Scanners scanners, String dataset,
      String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldLastUpdate(scanners.index(NB_QUERY_THREADS), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get the number of distinct terms for a given field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return cardinality estimation. No particular order should be expected from the returned
   *         iterator.
   */
  @Beta
  public Iterator<FieldDistinctTerms> fieldCardinalityEstimationForTerms(Scanners scanners,
      String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldCardinalityEstimationForTerms(scanners.index(NB_QUERY_THREADS), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get the number of distinct buckets for a given field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return cardinality estimation. No particular order should be expected from the returned
   *         iterator.
   */
  @Beta
  public Iterator<FieldDistinctBuckets> fieldCardinalityEstimationForBuckets(Scanners scanners,
      String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldCardinalityEstimationForBuckets(scanners.index(NB_QUERY_THREADS),
        dataset, field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get the number of distinct buckets for a given field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return top terms. No particular order should be expected from the returned iterator.
   */
  @Beta
  public Iterator<FieldTopTerms> fieldTopTerms(Scanners scanners, String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldTopTerms(scanners.index(NB_QUERY_THREADS), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get all JSON from the blob storage layer. Note that using a BatchScanner improves performances
   * a lot.
   *
   * The <dataset>_RAW_DATA auth is not enough to get access to the full JSON document. The user
   * must also have the <dataset>_<field> auth for each requested field.
   * 
   * @param scanners scanners.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @param nbQueryThreads JSON fields to keep (optional).
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public Iterator<Blob<Value>> jsonScan(Scanners scanners, String dataset, Set<String> fields,
      int nbQueryThreads) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return blobStore_.get(scanners.blob(nbQueryThreads), dataset, null, fields);
  }

  /**
   * Get JSON from the blob storage layer.
   *
   * The <dataset>_RAW_DATA auth is not enough to get access to the full JSON document. The user
   * must also have the <dataset>_<field> auth for each requested field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param fields JSON fields to keep (optional).
   * @param docsIds documents unique identifiers.
   * @param nbQueryThreads JSON fields to keep (optional).
   * @return list of documents. No particular order should be expected from the returned iterator if
   *         {@code nbQueryThreads} is set to a value above 1.
   */
  public Iterator<Blob<Value>> jsonScan(Scanners scanners, String dataset, Set<String> fields,
      Set<String> docsIds, int nbQueryThreads) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docsIds, "docsIds should not be null");

    return blobStore_.get(scanners.blob(nbQueryThreads), dataset, docsIds, fields);
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of a given term.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @return the estimated number of occurrences of the given term.
   */
  public long termCardinalityEstimationForBuckets(Scanners scanners, String dataset,
      Set<String> fields, String term) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(
        !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term)),
        "term cannot start AND end with a wildcard");

    @Var
    long count = 0;

    Iterator<TermDistinctBuckets> iter = termStore_.termCardinalityEstimationForBuckets(
        scanners.index(NB_QUERY_THREADS), dataset, fields, term);

    while (iter.hasNext()) {
      TermDistinctBuckets termCount = iter.next();
      count += termCount.count();
    }
    return count;
  }

  /**
   * Estimate the number of buckets with at least one of occurrence of all terms in [minTerm,
   * maxTerm].
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @return the estimated number of terms in [minTerm, maxTerm].
   */
  public long termCardinalityEstimationForBuckets(Scanners scanners, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    @Var
    long count = 0;

    Iterator<TermDistinctBuckets> iter = termStore_.termCardinalityEstimationForBuckets(
        scanners.index(NB_QUERY_THREADS), dataset, fields, minTerm, maxTerm);

    while (iter.hasNext()) {
      TermDistinctBuckets termCount = iter.next();
      count += termCount.count();
    }
    return count;
  }

  /**
   * Get the ids of all documents where at least one token matches "term".
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public Iterator<String> docsIds(Scanners scanners, Writers writers, String dataset, String term,
      Set<String> fields, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    // Build a cache key
    List<String> params = Lists.newArrayList(Strings.nullToEmpty(dataset), term);
    params.addAll(Splitter.on(',').trimResults().omitEmptyStrings()
        .splitToList(scanners.index().getAuthorizations().toString()));

    if (fields != null) {
      params.addAll(fields);
    }
    if (docsIds != null) {
      params.add(BloomFilters.toString(docsIds));
    }

    Collections.sort(params);

    String cacheId = hashFunction_.newHasher()
        .putString(Joiner.on(SEPARATOR_NUL).join(params), StandardCharsets.UTF_8).hash().toString();

    if (DataStoreCache.hasData(scanners, cacheId)) {
      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
            .add("cache_hit", true).add("cache_id", cacheId).formatDebug());
      }
    } else {
      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
            .add("cache_miss", true).add("cache_id", cacheId).formatDebug());
      }

      // Extract buckets ids, i.e. documents ids, from the TermStore and cache them
      Iterator<String> bucketsIds = Iterators.transform(
          termStore_.bucketsIds(scanners.index(NB_QUERY_THREADS), dataset, fields, term, docsIds),
          t -> t.bucketId() + SEPARATOR_NUL + t.dataset());

      DataStoreCache.write(scanners, writers, cacheId, bucketsIds);
    }

    // Returns an iterator over the documents ids
    return DataStoreCache.read(scanners, cacheId);
  }

  /**
   * Get the ids of all documents where at least one token matches a term in [minTerm, maxTerm].
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public Iterator<String> docsIds(Scanners scanners, Writers writers, String dataset,
      Set<String> fields, Object minTerm, Object maxTerm, BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    // Build a cache key
    List<String> params = Lists.newArrayList(Strings.nullToEmpty(dataset));
    params.addAll(Splitter.on(',').trimResults().omitEmptyStrings()
        .splitToList(scanners.index().getAuthorizations().toString()));

    if (minTerm != null) {
      params.add(minTerm.toString());
    }
    if (maxTerm != null) {
      params.add(maxTerm.toString());
    }
    if (fields != null) {
      params.addAll(fields);
    }
    if (docsIds != null) {
      params.add(BloomFilters.toString(docsIds));
    }

    Collections.sort(params);

    String cacheId = hashFunction_.newHasher()
        .putString(Joiner.on(SEPARATOR_NUL).join(params), StandardCharsets.UTF_8).hash().toString();

    if (DataStoreCache.hasData(scanners, cacheId)) {
      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
            .add("cache_hit", true).add("cache_id", cacheId).formatDebug());
      }
    } else {
      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
            .add("cache_miss", true).add("cache_id", cacheId).formatDebug());
      }

      // Extract buckets ids, i.e. documents ids, from the TermStore and cache them
      Iterator<String> bucketsIds =
          Iterators.transform(termStore_.bucketsIds(scanners.index(NB_QUERY_THREADS), dataset,
              fields, minTerm, maxTerm, docsIds), t -> t.bucketId() + SEPARATOR_NUL + t.dataset());

      DataStoreCache.write(scanners, writers, cacheId, bucketsIds);
    }

    // Returns an iterator over the documents ids
    return DataStoreCache.read(scanners, cacheId);
  }

  /**
   * Return misc. infos about a given list of datasets.
   *
   * @param datasets a list of datasets.
   * @param auths the user authorizations.
   * @return {@link DataStoreInfos}.
   */
  @Beta
  public DataStoreInfos infos(Set<String> datasets, Authorizations auths) {

    DataStoreInfos infos = new DataStoreInfos(name());

    try (Scanners scanners = scanners(auths)) {

      datasets.forEach(dataset -> {

        Iterator<FieldDistinctTerms> cardEstForTermsIterator =
            fieldCardinalityEstimationForTerms(scanners, dataset, null);

        while (cardEstForTermsIterator.hasNext()) {
          FieldDistinctTerms distinctTerms = cardEstForTermsIterator.next();
          infos.addCardinalityEstimationForTerms(dataset, distinctTerms.field(),
              distinctTerms.type(), distinctTerms.estimate());
        }

        Iterator<FieldDistinctBuckets> cardEstForBucketsIterator =
            fieldCardinalityEstimationForBuckets(scanners, dataset, null);

        while (cardEstForBucketsIterator.hasNext()) {
          FieldDistinctBuckets distinctBuckets = cardEstForBucketsIterator.next();
          infos.addCardinalityEstimationForBuckets(dataset, distinctBuckets.field(),
              distinctBuckets.type(), distinctBuckets.estimate());
        }

        Iterator<FieldTopTerms> topTermsIterator = fieldTopTerms(scanners, dataset, null);

        while (topTermsIterator.hasNext()) {
          FieldTopTerms topTerms = topTermsIterator.next();
          infos.addTopTermsNoFalsePositives(dataset, topTerms.field(), topTerms.type(),
              topTerms.topTermsNoFalsePositives());
          infos.addTopTermsNoFalseNegatives(dataset, topTerms.field(), topTerms.type(),
              topTerms.topTermsNoFalseNegatives());
        }

        Iterator<FieldLabels> labelsIterator = fieldVisibilityLabels(scanners, dataset, null);

        while (labelsIterator.hasNext()) {
          FieldLabels labels = labelsIterator.next();
          infos.addVisibilityLabels(dataset, labels.field(), labels.type(), labels.termLabels());
        }

        Iterator<FieldLastUpdate> lastUpdateIterator = fieldLastUpdate(scanners, dataset, null);

        while (lastUpdateIterator.hasNext()) {
          FieldLastUpdate lastUpdate = lastUpdateIterator.next();
          infos.addLastUpdate(dataset, lastUpdate.field(), lastUpdate.type(),
              lastUpdate.lastUpdate());
        }
      });
    }
    return infos;
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param dataset the dataset.
   * @param docId the document identifier.
   * @param blob the JSON string.
   * @return true if the write operation succeeded, false otherwise.
   */
  private boolean persistBlob(Writers writers, String dataset, String docId, String blob) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(blob, "blob should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("doc_id", docId).add("blob", blob).formatDebug());
    }

    String vizAdm = STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(docId);
    String vizRawData = vizDataset + STRING_RAW_DATA;

    if (!blobStore_.putJson(writers.blob(), dataset, docId,
        Sets.newHashSet(vizAdm, vizUuid, vizRawData), blob)) {

      logger_.error(LogFormatter.create(true).message("write failed").add("dataset", dataset)
          .add("docId", docId).add("blob", blob).formatError());

      return false;
    }
    return true;
  }

  /**
   * Persist a single term.
   *
   * @param writers writers.
   * @param dataset the dataset.
   * @param docId the document identifier.
   * @param field the field name.
   * @param term the term to index.
   * @param nbOccurrencesInDoc the number of occurrences of the term in the document.
   * @return true if the write operation succeeded, false otherwise.
   */
  private boolean persistTerm(Writers writers, String dataset, String docId, String field,
      Object term, int nbOccurrencesInDoc) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrencesInDoc > 0, "nbOccurrencesInDoc must be > 0");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", name()).add("dataset", dataset)
          .add("doc_id", docId).add("field", field).add("term", term)
          .add("nb_occurrences_in_doc", nbOccurrencesInDoc).formatDebug());
    }

    List<String> path =
        Splitter.on(SEPARATOR_CURRENCY_SIGN).trimResults().omitEmptyStrings().splitToList(field);

    String vizAdm = STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(docId);

    Set<String> vizDocSpecific = Sets.newHashSet(vizUuid);
    Set<String> vizFieldSpecific = Sets.newHashSet(vizAdm);

    AbstractStorage.toVisibilityLabels(path)
        .forEach(label -> vizFieldSpecific.add(vizDataset + label));

    boolean isOk = termStore_.put(writers.index(), dataset, docId, field, term, nbOccurrencesInDoc,
        vizDocSpecific, vizFieldSpecific);

    if (!isOk) {
      logger_.error(LogFormatter.create(true).message("write failed").add("dataset", dataset)
          .add("docId", docId).add("field", field).add("term", term).formatError());
    }
    return isOk;
  }
}
