package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.filters.AgeOffPeriodFilter;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.DedupIterator;
import com.computablefacts.jupiter.storage.FlattenIterator;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.jupiter.storage.termstore.FieldCard;
import com.computablefacts.jupiter.storage.termstore.FieldCount;
import com.computablefacts.jupiter.storage.termstore.FieldLabels;
import com.computablefacts.jupiter.storage.termstore.IngestStats;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.computablefacts.jupiter.storage.termstore.TermCard;
import com.computablefacts.jupiter.storage.termstore.TermCount;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.computablefacts.nona.types.Span;
import com.computablefacts.nona.types.SpanSequence;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * This storage layer acts as a generic data store. For now, this storage layer is mostly used to
 * persist/retrieve JSON objects. Raw JSON objects are stored to a {@link BlobStore}. Indexed terms
 * are stored to a {@link BlobStore}.
 * </p>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class DataStore {

  private static final Logger logger_ = LoggerFactory.getLogger(DataStore.class);
  private static final ExecutorService executorService_ = Executors.newFixedThreadPool(3);

  private final String name_;
  private final BlobStore blobStore_;
  private final TermStore termStore_;

  public DataStore(Configurations configurations, String name) {
    name_ = Preconditions.checkNotNull(name, "name should neither be null nor empty");
    blobStore_ = new BlobStore(configurations, blobStoreName(name));
    termStore_ = new TermStore(configurations, termStoreName(name));
  }

  @Generated
  static String blobStoreName(String name) {
    return name + "Blobs";
  }

  @Generated
  static String termStoreName(String name) {
    return name + "Terms";
  }

  private static void writeCache(Writers writers, Iterator<String> iterator, Text uuid,
      @Var int maxElementsToWrite) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");
    Preconditions.checkNotNull(uuid, "uuid should not be null");

    try {
      if (maxElementsToWrite < 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(uuid);
          mutation.put(Constants.TEXT_CACHE, new Text(Strings.nullToEmpty(value)),
              Constants.VALUE_EMPTY);

          writers.blob().addMutation(mutation);
        }
      } else if (maxElementsToWrite > 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(uuid);
          mutation.put(Constants.TEXT_CACHE, new Text(Strings.nullToEmpty(value)),
              Constants.VALUE_EMPTY);

          writers.blob().addMutation(mutation);

          if (--maxElementsToWrite <= 0) {
            break;
          }
        }
      } else {
        logger_.warn(
            LogFormatterManager.logFormatter().message("write ignored").add("uuid", uuid.toString())
                .add("max_elements_to_write", maxElementsToWrite).formatWarn());
      }

      // flush otherwise mutations might not have been written when readCache() is called
      writers.flush();
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    }
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
   * Get batch scanners.
   *
   * @return scanners.
   */
  @Deprecated
  public Scanners batchScanners(Authorizations authorizations) {
    return new Scanners(configurations(), name(), authorizations, 5);
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

  /**
   * Get stats writer.
   *
   * @return writer.
   */
  @Deprecated
  public IngestStats newIngestStats() {
    return new IngestStats(termStore_, termStore_.writer());
  }

  @Deprecated
  public boolean grantWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean grantReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean grantWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean grantReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.grantPermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean revokeWritePermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean revokeReadPermissionOnBlobStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(blobStore_.configurations().connector(), username,
        blobStoreName(name()), TablePermission.READ);
  }

  @Deprecated
  public boolean revokeWritePermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

    return Users.revokePermission(termStore_.configurations().connector(), username,
        termStoreName(name()), TablePermission.WRITE);
  }

  @Deprecated
  public boolean revokeReadPermissionOnTermStore(String username) {

    Preconditions.checkNotNull(username, "username should not be null");

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

    return revokeReadPermissionOnBlobStore(username) && revokeReadPermissionOnTermStore(username);
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {
    return blobStore_.isReady() && termStore_.isReady();
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  public boolean create() {

    boolean isReady = blobStore_.isReady() && termStore_.isReady();

    if (!isReady && blobStore_.create() && termStore_.create()) {
      try {

        // Set a 3 hours TTL on all cached data
        IteratorSetting settings = new IteratorSetting(7, AgeOffPeriodFilter.class);
        AgeOffPeriodFilter.setColumnFamily(settings, Constants.TEXT_CACHE.toString());
        AgeOffPeriodFilter.setTtl(settings, 3);
        AgeOffPeriodFilter.setTtlUnits(settings, "HOURS");

        configurations().tableOperations().attachIterator(blobStore_.tableName(), settings);

      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).formatInfo());
    }

    @Var
    boolean isOk = true;
    Authorizations auths = new Authorizations(Constants.STRING_ADM);

    try (BatchDeleter deleter = termStore_.deleter(auths)) {
      isOk = isOk && termStore_.removeDataset(deleter, dataset);
    }
    try (BatchDeleter deleter = blobStore_.deleter(auths)) {
      isOk = isOk && blobStore_.removeDataset(deleter, dataset);
    }
    return isOk;
  }

  /**
   * Remove documents from a given dataset. This method does not update the *CARD et *CNT datasets.
   * Hence, cardinalities and counts may become out of sync.
   *
   * @param dataset dataset.
   * @param docIds a set of documents ids to remove.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean remove(String dataset, Set<String> docIds) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docIds, "docIds should not be null");
    Preconditions.checkArgument(!docIds.isEmpty(), "docIds should contain at least one id");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).add("doc_ids", docIds)
          .formatInfo());
    }

    @Var
    boolean isOk = true;
    Authorizations auths = new Authorizations(Constants.STRING_ADM);

    try (BatchDeleter deleter = termStore_.deleter(auths)) {
      isOk = isOk && termStore_.removeDocuments(deleter, dataset, docIds);
    }
    try (BatchDeleter deleter = blobStore_.deleter(auths)) {
      isOk = isOk && blobStore_.removeKeys(deleter, dataset, docIds);
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
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).formatInfo());
    }

    boolean isOk1 = blobStore_.addLocalityGroup(dataset);
    boolean isOk2 = termStore_.addLocalityGroup(dataset);

    return isOk1 && isOk2;
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param json JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, IngestStats stats, String dataset, String uuid,
      String json) {
    return persist(writers, stats, dataset, uuid, json, key -> true, Codecs.nopTokenizer,
        Codecs.nopLexicoder);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param json JSON object.
   * @param keepField filter applied on all JSON attributes before value tokenization (optional).
   *        This predicate should return true iif the field's value must be tokenized.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, IngestStats stats, String dataset, String uuid,
      String json, Predicate<String> keepField) {
    return persist(writers, stats, dataset, uuid, json, keepField, Codecs.nopTokenizer,
        Codecs.nopLexicoder);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param json JSON object.
   * @param keepField filter applied on all JSON attributes before value tokenization (optional).
   *        This predicate should return true iif the field's value must be tokenized.
   * @param tokenizer string tokenizer (optional).
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, IngestStats stats, String dataset, String uuid,
      String json, Predicate<String> keepField, Function<String, SpanSequence> tokenizer) {
    return persist(writers, stats, dataset, uuid, json, keepField, tokenizer, Codecs.nopLexicoder);
  }

  /**
   * Persist a single JSON object.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param json JSON object.
   * @param keepField filter applied on all JSON attributes before value tokenization (optional).
   *        This predicate should return true iif the field's value must be tokenized.
   * @param tokenizer string tokenizer (optional).
   * @param lexicoder represents java Objects as sortable strings.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persist(Writers writers, IngestStats stats, String dataset, String uuid,
      String json, Predicate<String> keepField, Function<String, SpanSequence> tokenizer,
      Function<Object, Span> lexicoder) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(uuid, "uuid should not be null");
    Preconditions.checkNotNull(json, "json should not be null");
    Preconditions.checkNotNull(lexicoder, "lexicoder should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("dataset", dataset).add("uuid", uuid)
          .add("json", json).add("has_keep_field", keepField != null)
          .add("has_tokenizer", tokenizer != null).add("has_lexicoder", lexicoder != null)
          .formatDebug());
    }

    if (!persistBlob(writers, stats, dataset, uuid, json)) {
      return false;
    }

    @Var
    SpanSequence spanSequence = null;
    Map<String, Object> newJson =
        new JsonFlattener(json).withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).flattenAsMap();

    for (String field : newJson.keySet()) {

      spanSequence = null; // free memory

      // Attributes starting with an underscore should not be indexed
      if (field.startsWith("_") || field.contains(Constants.SEPARATOR_CURRENCY_SIGN + "_")) {
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

      @Var
      boolean writeInForwardIndexOnly = false;

      if (tokenizer == null || !(value instanceof String)) {

        // Objects other than String are lexicoded
        spanSequence = new SpanSequence();
        spanSequence.add(Objects.requireNonNull(lexicoder.apply(value)));
        writeInForwardIndexOnly = !(value instanceof String);
      } else if (Codecs.isProbablyBase64((String) value)) {

        // Base64 strings are NOT indexed
        continue;
      } else {
        spanSequence = Objects.requireNonNull(tokenizer.apply((String) value));
      }

      // Group by spans
      Map<String, List<Pair<Integer, Integer>>> spans = new HashMap<>();

      for (Span span : spanSequence) {

        String text = span.text();

        if (!spans.containsKey(text)) {
          spans.put(text, new ArrayList<>());
        }
        spans.get(text).add(new Pair<>(span.begin(), span.end()));
      }

      spanSequence = null; // free memory

      // Increment field cardinality
      if (stats != null) {
        stats.card(dataset, field, 1);
      }

      // Persist spans
      for (String span : spans.keySet()) {
        if (!persistTerm(writers, stats, dataset, uuid, field, span, spans.get(span),
            writeInForwardIndexOnly)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Persist a single blob.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param blob blob encoded in Base64.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persistBlob(Writers writers, IngestStats stats, String dataset, String uuid,
      String blob) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(uuid, "uuid should not be null");
    Preconditions.checkNotNull(blob, "blob should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("dataset", dataset).add("uuid", uuid)
          .add("blob", blob).formatDebug());
    }

    String vizAdm = Constants.STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(uuid);
    String vizRawData = vizDataset + Constants.STRING_RAW_DATA;

    if (!blobStore_.put(writers.blob(), dataset, uuid, Sets.newHashSet(vizAdm, vizUuid, vizRawData),
        blob)) {

      logger_.error(LogFormatterManager.logFormatter().message("write failed")
          .add("dataset", dataset).add("uuid", uuid).add("blob", blob).formatError());

      return false;
    }

    // Increment blob count
    if (stats != null) {
      stats.count(dataset, "", 1);
      stats.card(dataset, "", 1);
      stats.visibility(dataset, "", Sets.newHashSet(vizAdm, vizRawData));
    }
    return true;
  }

  /**
   * Persist a single term.
   *
   * @param writers writers.
   * @param stats ingest stats (optional)
   * @param dataset dataset.
   * @param uuid unique identifier.
   * @param field field name.
   * @param term term.
   * @param spans positions of the term in the document.
   * @param writeInForwardIndexOnly allow the caller to explicitly specify that the term must be
   *        written in the forward index only.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean persistTerm(Writers writers, IngestStats stats, String dataset, String uuid,
      String field, String term, List<Pair<Integer, Integer>> spans,
      boolean writeInForwardIndexOnly) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(uuid, "uuid should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkNotNull(spans, "spans should not be null");

    String vizAdm = Constants.STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(uuid);

    int index = field.indexOf(Constants.SEPARATOR_CURRENCY_SIGN);
    String vizField = AbstractStorage
        .toVisibilityLabel(vizDataset + (index <= 0 ? field : field.substring(0, index)));

    Set<String> vizDocSpecific = Sets.newHashSet(vizUuid);
    Set<String> vizFieldSpecific = Sets.newHashSet(vizAdm, vizField);

    boolean isOk = termStore_.add(writers.index(), stats, dataset, uuid, field, term, spans,
        vizDocSpecific, vizFieldSpecific, writeInForwardIndexOnly);

    if (!isOk) {
      logger_
          .error(LogFormatterManager.logFormatter().message("write failed").add("dataset", dataset)
              .add("uuid", uuid).add("field", field).add("term", term).formatError());
    }

    if (stats != null) {

      // Do not store visibility labels generated from the document UUID because there is one
      // for each document
      stats.removeVisibilityLabel(dataset, field, vizUuid);
    }
    return isOk;
  }

  /**
   * Get visibility labels by field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return visibility labels.
   */
  public Iterator<FieldLabels> fieldLabels(Scanners scanners, String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldLabels(scanners.index(), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get count by field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return count.
   */
  public Iterator<FieldCount> fieldCount(Scanners scanners, String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldCount(scanners.index(), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get cardinality by field.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field field.
   * @return count.
   */
  public Iterator<FieldCard> fieldCard(Scanners scanners, String dataset, String field) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return termStore_.fieldCard(scanners.index(), dataset,
        field == null ? null : Sets.newHashSet(field));
  }

  /**
   * Get count by term.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param term term.
   * @return count.
   */
  public Iterator<Pair<String, List<TermCount>>> termCount(Scanners scanners, String dataset,
      String term) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(scanners.index() instanceof Scanner,
        "index scanner must guarantee the result order");

    return termStore_.termCount((Scanner) scanners.index(), dataset, term);
  }

  /**
   * Get cardinality by term.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param term term.
   * @return count.
   */
  public Iterator<Pair<String, List<TermCard>>> termCard(Scanners scanners, String dataset,
      String term) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(scanners.index() instanceof Scanner,
        "index scanner must guarantee the result order");

    return termStore_.termCard((Scanner) scanners.index(), dataset, term);
  }

  /**
   * Get UUIDs grouped by terms.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param term term.
   * @return iterator.
   */
  public Iterator<Pair<String, List<Term>>> termScan(Scanners scanners, String dataset,
      String term) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    return termScan(scanners, dataset, term, null, null);
  }

  /**
   * Get UUIDs grouped by terms.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param term term.
   * @param keepFields fields patterns to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return iterator.
   */
  public Iterator<Pair<String, List<Term>>> termScan(Scanners scanners, String dataset, String term,
      Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(scanners.index() instanceof Scanner,
        "index scanner must guarantee the result order");

    return termStore_.termScan((Scanner) scanners.index(), dataset, term, keepFields, keepDocs);
  }

  /**
   * Get UUIDs ordered in lexicographic order.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param minTerm number (optional). Beginning of the range (included).
   * @param maxTerm number (optional). End of the range (included).
   * @return iterator.
   */
  public Iterator<Term> numericalRangeScan(Scanners scanners, String dataset, String minTerm,
      String maxTerm) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(scanners.index() instanceof Scanner,
        "index scanner must guarantee the result order");

    return numericalRangeScan(scanners, dataset, minTerm, maxTerm, null, null);
  }

  /**
   * Get UUIDs ordered in lexicographic order.
   *
   * @param scanners scanners.
   * @param dataset dataset (optional).
   * @param minTerm number (optional). Beginning of the range (included).
   * @param maxTerm number (optional). End of the range (excluded).
   * @param keepFields fields patterns to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return iterator.
   */
  public Iterator<Term> numericalRangeScan(Scanners scanners, String dataset, String minTerm,
      String maxTerm, Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(scanners.index() instanceof Scanner,
        "index scanner must guarantee the result order");

    return termStore_.numericalRangeScan((Scanner) scanners.index(), dataset, minTerm, maxTerm,
        keepFields, keepDocs);
  }

  /**
   * Get all blobs from the blob storage layer. Note that using a BatchScanner improves performances
   * a lot.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @return list of documents.
   */
  public Iterator<Blob<Value>> blobScan(Scanners scanners, String dataset) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return blobStore_.get(scanners.blob(), dataset);
  }

  /**
   * Get blobs from the blob storage layer.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param uuids documents unique identifiers.
   * @return list of documents.
   */
  public Iterator<Blob<Value>> blobScan(Scanners scanners, String dataset, Set<String> uuids) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(uuids, "uuids should not be null");

    return blobStore_.get(scanners.blob(), dataset, uuids);
  }

  /**
   * Get the ids of all documents for which a numerical term is in a given range.
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset.
   * @param minTerm number (optional). Beginning of the range (included).
   * @param maxTerm number (optional). End of the range (included).
   * @param keepFields fields to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return doc ids. Ids are sorted if and only if the {@link Scanners} class use
   *         {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner} underneath.
   */
  @Beta
  public Iterator<String> searchByNumericalRange(Scanners scanners, Writers writers, String dataset,
      String minTerm, String maxTerm, Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");

    if (logger_.isInfoEnabled()) {
      logger_
          .info(LogFormatterManager.logFormatter().add("dataset", dataset).add("min_term", minTerm)
              .add("max_term", maxTerm).add("has_keep_fields", keepFields != null)
              .add("has_keep_docs", keepDocs != null).formatInfo());
    }

    // TODO : backport code in order to avoid this write/read trick (sort doc ids)
    return readCache(scanners,
        writeCache(writers,
            new DedupIterator<>(Iterators.transform(
                numericalRangeScan(scanners, dataset, minTerm, maxTerm, keepFields, keepDocs),
                Term::docId))));
  }

  /**
   * Get the ids of all documents which contain a given term.
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset.
   * @param term a single term to match.
   * @param keepFields fields to keep (optional).
   * @return doc ids. Ids are sorted if and only if the {@link Scanners} class use
   *         {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner} underneath.
   */
  public Iterator<String> searchByTerm(Scanners scanners, Writers writers, String dataset,
      String term, Set<String> keepFields) {
    return searchByTerms(scanners, writers, dataset, Sets.newHashSet(term), keepFields, null);
  }

  /**
   * Get the ids of all documents which contain a given term.
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset.
   * @param term a single term to match.
   * @param keepFields fields to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return doc ids. Ids are sorted if and only if the {@link Scanners} class use
   *         {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner} underneath.
   */
  public Iterator<String> searchByTerm(Scanners scanners, Writers writers, String dataset,
      String term, Set<String> keepFields, BloomFilters<String> keepDocs) {
    return searchByTerms(scanners, writers, dataset, Sets.newHashSet(term), keepFields, keepDocs);
  }

  /**
   * Get the ids of all documents which contain a list of terms (in any order of appearance).
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset.
   * @param terms one or more terms to match.
   * @param keepFields fields to keep (optional).
   * @return doc ids. Ids are sorted if and only if the {@link Scanners} class use
   *         {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner} underneath.
   */
  public Iterator<String> searchByTerms(Scanners scanners, Writers writers, String dataset,
      Collection<String> terms, Set<String> keepFields) {
    return searchByTerms(scanners, writers, dataset, terms, keepFields, null);
  }

  /**
   * Get the ids of all documents which contain a list of terms (in any order of appearance).
   *
   * @param scanners scanners.
   * @param writers writers.
   * @param dataset dataset.
   * @param terms one or more terms to match.
   * @param keepFields fields to keep (optional).
   * @param keepDocs document ids to keep (optional).
   * @return doc ids. Ids are sorted if and only if the {@link Scanners} class use
   *         {@link org.apache.accumulo.core.client.Scanner} instead of
   *         {@link org.apache.accumulo.core.client.BatchScanner} underneath.
   */
  public Iterator<String> searchByTerms(Scanners scanners, Writers writers, String dataset,
      Collection<String> terms, Set<String> keepFields, BloomFilters<String> keepDocs) {

    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(terms, "terms should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).add("terms", terms)
          .add("has_keep_fields", keepFields != null).add("has_keep_docs", keepDocs != null)
          .formatInfo());
    }

    // Sort terms by decreasing length
    ToIntFunction<String> byTermLength = term -> {
      if (WildcardMatcher.startsWithWildcard(term)) {
        return WildcardMatcher.prefix(reverse(term)).length();
      }
      return WildcardMatcher.prefix(term).length();
    };

    List<String> newTerms = terms.stream()
        .filter(term -> !(WildcardMatcher.startsWithWildcard(term)
            && WildcardMatcher.endsWithWildcard(term)))
        .sorted(Comparator.comparingInt(byTermLength).reversed()).collect(Collectors.toList());

    if (newTerms.isEmpty()) {
      if (logger_.isWarnEnabled()) {
        logger_.warn(LogFormatterManager.logFormatter().message("all terms have been pruned")
            .add("dataset", dataset).add("terms", terms).add("has_keep_fields", keepFields != null)
            .add("has_keep_docs", keepDocs != null).formatWarn());
      }
      return Constants.ITERATOR_EMPTY;
    }

    // First, fill a Bloom filter with the UUIDs of the documents. Then, filter subsequent
    // terms using the Bloom filter created with the previous term.
    @Var
    BloomFilters<String> newKeepDocs = keepDocs == null ? null : new BloomFilters<>(keepDocs);

    for (int i = 0; i < newTerms.size() - 1; i++) {

      // TODO : if terms is a sorted Collection, ensure that the order of appearance is respected.

      Iterator<Pair<String, List<Term>>> iter =
          termScan(scanners, dataset, newTerms.get(0), keepFields, newKeepDocs);

      if (!iter.hasNext()) {
        return Constants.ITERATOR_EMPTY;
      }

      newKeepDocs = new BloomFilters<>();

      while (iter.hasNext()) {

        Pair<String, List<Term>> pair = iter.next();

        for (Term term : pair.getSecond()) {
          newKeepDocs.put(term.docId());
        }
      }
    }

    Iterator<Pair<String, List<Term>>> iter =
        termScan(scanners, dataset, newTerms.get(newTerms.size() - 1), keepFields, newKeepDocs);

    // TODO : backport code in order to avoid this write/read trick (sort doc ids)
    return readCache(scanners, writeCache(writers, new DedupIterator<>(
        Iterators.transform(new FlattenIterator<>(iter, Pair::getSecond), Term::docId))));
  }

  /**
   * Get a list of values.
   *
   * @param scanners scanners.
   * @param cacheId the cache id.
   * @return a list of values.
   */
  public Iterator<String> readCache(Scanners scanners, String cacheId) {
    return readCache(scanners, cacheId, null);
  }

  /**
   * Get a list of values.
   *
   * @param scanners scanners.
   * @param cacheId the cache id.
   * @param nextValue where to start iterating.
   * @return a list of values.
   */
  public Iterator<String> readCache(Scanners scanners, String cacheId, String nextValue) {

    Preconditions.checkNotNull(scanners, "scanners should neither be null nor empty");
    Preconditions.checkNotNull(cacheId, "cacheId should neither be null nor empty");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("cache_id", cacheId).formatInfo());
    }

    scanners.blob().clearColumns();
    scanners.blob().clearScanIterators();
    scanners.blob().fetchColumnFamily(Constants.TEXT_CACHE);

    Range range;

    if (nextValue == null) {
      range = Range.exact(cacheId);
    } else {
      Key begin = new Key(new Text(cacheId), Constants.TEXT_CACHE, new Text(nextValue));
      Key end = begin.followingKey(PartialKey.ROW);
      range = new Range(begin, true, end, false);
    }

    if (!AbstractStorage.setRange(scanners.blob(), range)) {
      return Constants.ITERATOR_EMPTY;
    }
    return Iterators.transform(scanners.blob().iterator(),
        entry -> entry.getKey().getColumnQualifier().toString());
  }

  /**
   * Write a list of values.
   *
   * @param writers writers.
   * @param iterator values.
   * @return a cache id.
   */
  public String writeCache(Writers writers, Iterator<String> iterator) {
    return writeCache(writers, iterator, -1);
  }

  /**
   * Write a list of values.
   *
   * @param delegateToBackgroundThreadAfter synchronously write to cache until this number of
   *        elements is reached. After that, delegate the remaining writes to a background thread.
   *        If this number is less than or equals to zero, performs the whole operation
   *        synchronously.
   * @param writers writers.
   * @param iterator values.
   * @return a cache id.
   */
  public String writeCache(Writers writers, Iterator<String> iterator,
      @Var int delegateToBackgroundThreadAfter) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    Text uuid = new Text(UUID.randomUUID().toString());

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("cache_id", uuid).formatInfo());
    }

    if (delegateToBackgroundThreadAfter <= 0) {
      writeCache(writers, iterator, uuid, -1);
    } else {
      writeCache(writers, iterator, uuid, delegateToBackgroundThreadAfter);
      executorService_
          .execute(() -> writeCache(writers, iterator, uuid, delegateToBackgroundThreadAfter));
    }
    return uuid.toString();
  }
}
