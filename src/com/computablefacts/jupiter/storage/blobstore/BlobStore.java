package com.computablefacts.jupiter.storage.blobstore;

import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.asterix.Generated;
import com.computablefacts.asterix.View;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.OrderedView;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.UnorderedView;
import com.computablefacts.jupiter.combiners.BlobStoreCombiner;
import com.computablefacts.jupiter.filters.BlobStoreMaskedJsonFieldFilter;
import com.computablefacts.jupiter.iterators.BlobStoreFilterOutJsonFieldsIterator;
import com.computablefacts.jupiter.iterators.BlobStoreMaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * The BlobStore API allows your application to persist data objects. Methods are available to write
 * and read opaque Strings, JSON and Files.
 * </p>
 *
 * <p>
 * This storage layer acts as a blob store. The storage layer utilizes the
 * <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as the basis for
 * its ingest and query components.
 * </p>
 * 
 * <pre>
 *  Row               | Column Family | Column Qualifier                   | Visibility                             |Value
 * ===================+===============+====================================+========================================+========
 *  <dataset>\0<key>  | <blob_type>   | <property_1>\0<property_2>\0...    | ADM|<dataset>_RAW_DATA|<dataset>_<key> | <blob>
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class BlobStore extends AbstractStorage {

  public static final String TYPE_STRING = "STR";
  public static final String TYPE_FILE = "FIL";
  public static final String TYPE_JSON = "JSO";
  public static final String TYPE_ARRAY = "ARR";

  private static final int BLOBSTORE_COMBINER_PRIORITY = 10;
  private static final int FILTER_OUT_JSON_FIELDS_ITERATOR_PRIORITY = 30;
  private static final int MASKING_ITERATOR_PRIORITY = 31;
  private static final int MASKED_JSON_FIELD_FILTER_ITERATOR_PRIORITY = 32;
  private static final int MAX_NUMBER_OF_SHARDS = 100;
  private static final Set<String> ARRAY_SHARDS;

  private static final Logger logger_ = LoggerFactory.getLogger(BlobStore.class);

  static {
    Set<String> set = new HashSet<>();
    set.add(TYPE_ARRAY); // for backward compatibility only
    for (int i = 0; i < MAX_NUMBER_OF_SHARDS; i++) {
      set.add(TYPE_ARRAY + (i < 10 ? "0" + i : i));
    }
    ARRAY_SHARDS = ImmutableSet.copyOf(set);
  }

  public BlobStore(Configurations configurations, String name) {
    super(configurations, name);
  }

  @Generated
  public static Set<String> allArrayShards() {
    return ARRAY_SHARDS;
  }

  /**
   * This method is here to ensure that rows sharing the same key belong to the same array shard.
   *
   * @param key the row key.
   * @return the array shard.
   */
  public static String arrayShard(String key) {

    Preconditions.checkNotNull(key, "key should not be null");

    int length = key.length();
    @Var
    long hash = 1125899906842597L; // prime

    for (int i = 0; i < length; i++) {
      hash = 31 * hash + key.charAt(i);
    }

    int shard = (int) ((hash < 0L ? -1L * hash : hash) % MAX_NUMBER_OF_SHARDS);
    return TYPE_ARRAY + (shard < 10 ? "0" + shard : shard);
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  @Override
  public boolean create() {

    if (!isReady()) {
      if (!super.create()) {
        return false;
      }
    }

    try {

      // Remove legacy iterators from the BlobStore
      Map<String, EnumSet<IteratorUtil.IteratorScope>> iterators =
          configurations().tableOperations().listIterators(tableName());

      if (iterators.containsKey("BlobStoreCombiner")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(),
            BlobStoreCombiner.class.getSimpleName(), EnumSet.of(IteratorUtil.IteratorScope.majc,
                IteratorUtil.IteratorScope.minc, IteratorUtil.IteratorScope.scan));
      }

      // Set the array combiner
      IteratorSetting settings =
          new IteratorSetting(BLOBSTORE_COMBINER_PRIORITY, BlobStoreCombiner.class);
      BlobStoreCombiner.setColumns(settings,
          allArrayShards().stream().map(IteratorSetting.Column::new).collect(Collectors.toList()));
      BlobStoreCombiner.setReduceOnFullCompactionOnly(settings, true);

      configurations().tableOperations().attachIterator(tableName(), settings);

      // Set locality groups
      Set<String> groups = new HashSet<>();
      groups.add(TYPE_STRING);
      groups.add(TYPE_FILE);
      groups.add(TYPE_JSON);
      groups.addAll(allArrayShards());

      return addLocalityGroups(groups);

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
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
    String end =
        begin.substring(0, begin.length() - 1) + (char) (begin.charAt(begin.length() - 1) + 1);

    return Tables.deleteRows(configurations().tableOperations(), tableName(), begin, end);
  }

  /**
   * Persist a file.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param file file to load and persist.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean putFile(BatchWriter writer, String dataset, String key, Set<String> labels,
      java.io.File file) {
    return add(writer, Blob.fromFile(dataset, key, labels, file));
  }

  /**
   * Persist a string.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param value string.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean putString(BatchWriter writer, String dataset, String key, Set<String> labels,
      String value) {
    return add(writer, Blob.fromString(dataset, key, labels, value));
  }

  /**
   * Persist a JSON object.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param value JSON object.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean putJson(BatchWriter writer, String dataset, String key, Set<String> labels,
      Map<String, Object> value) {
    return putJson(writer, dataset, key, labels, Codecs.asString(value));
  }

  /**
   * Persist a JSON string.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param value JSON string.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean putJson(BatchWriter writer, String dataset, String key, Set<String> labels,
      String value) {
    return add(writer, Blob.fromJson(dataset, key, labels, value));
  }

  /**
   * Persist a string. If two or more strings share the same key, they will be added to an array.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param value JSON string.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean putArray(BatchWriter writer, String dataset, String key, Set<String> labels,
      String value) {
    return add(writer, Blob.fromArray(dataset, key, labels, value));
  }

  /**
   * Get all blobs of STRING type (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> stringsSortedByKey(Authorizations authorizations, String dataset,
      Set<String> keys, Set<String> fields) {
    return get(scanner(authorizations), dataset, TYPE_STRING, keys, fields, null);
  }

  /**
   * Get all blobs of STRING type (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> strings(Authorizations authorizations, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(batchScanner(authorizations, NB_QUERY_THREADS), dataset, TYPE_STRING, keys, fields,
        null);
  }

  /**
   * Get all blobs of JSON type (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> jsonsSortedByKey(Authorizations authorizations, String dataset,
      Set<String> keys, Set<String> fields) {
    return get(scanner(authorizations), dataset, TYPE_JSON, keys, fields, null);
  }

  /**
   * Get all blobs of JSON type (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> jsons(Authorizations authorizations, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(batchScanner(authorizations, NB_QUERY_THREADS), dataset, TYPE_JSON, keys, fields,
        null);
  }

  /**
   * Get all blobs of JSON type (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @param hashes JSON fields filters (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> jsonsSortedByKey(Authorizations authorizations, String dataset,
      Set<String> keys, Set<String> fields, Set<Map.Entry<String, String>> hashes) {
    return get(scanner(authorizations), dataset, TYPE_JSON, keys, fields, hashes);
  }

  /**
   * Get all blobs of JSON type (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @param hashes JSON fields filters (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> jsons(Authorizations authorizations, String dataset, Set<String> keys,
      Set<String> fields, Set<Map.Entry<String, String>> hashes) {
    return get(batchScanner(authorizations, NB_QUERY_THREADS), dataset, TYPE_JSON, keys, fields,
        hashes);
  }

  /**
   * Get all blobs of FILE type (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> filesSortedByKey(Authorizations authorizations, String dataset,
      Set<String> keys, Set<String> fields) {
    return get(scanner(authorizations), dataset, TYPE_FILE, keys, fields, null);
  }

  /**
   * Get all blobs of FILE type (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> files(Authorizations authorizations, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(batchScanner(authorizations, NB_QUERY_THREADS), dataset, TYPE_FILE, keys, fields,
        null);
  }

  /**
   * Get all blobs of ARRAY type (sorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> arraysSortedByKey(Authorizations authorizations, String dataset,
      Set<String> keys, Set<String> fields) {
    return get(scanner(authorizations), dataset, TYPE_ARRAY, keys, fields, null);
  }

  /**
   * Get all blobs of ARRAY type (unsorted).
   *
   * @param authorizations authorizations.
   * @param dataset dataset/namespace.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @return an iterator of (key, value) pairs.
   */
  public View<Blob<Value>> arrays(Authorizations authorizations, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(batchScanner(authorizations, NB_QUERY_THREADS), dataset, TYPE_ARRAY, keys, fields,
        null);
  }

  /**
   * Get one or more blobs.
   *
   * The <dataset>_RAW_DATA authorization gives the user access to the full JSON document. If this
   * authorization is not specified, the user must have the <dataset>_<field> auth for each
   * requested field.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @param blobType the type of blob to retrieve.
   * @param keys keys (optional).
   * @param fields fields to keep if Accumulo Values are JSON objects (optional).
   * @param hashes JSON fields filters (optional).
   * @return an iterator of (key, value) pairs.
   */
  private View<Blob<Value>> get(ScannerBase scanner, String dataset, String blobType,
      Set<String> keys, Set<String> fields, Set<Map.Entry<String, String>> hashes) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(blobType, "blobType should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    scanner.clearColumns();
    scanner.clearScanIterators();

    if (TYPE_ARRAY.equals(blobType)) {
      allArrayShards().forEach(cf -> scanner.fetchColumnFamily(new Text(cf)));
    } else {
      scanner.fetchColumnFamily(new Text(blobType));
    }
    if (fields != null && !fields.isEmpty()) {

      IteratorSetting setting = new IteratorSetting(FILTER_OUT_JSON_FIELDS_ITERATOR_PRIORITY,
          BlobStoreFilterOutJsonFieldsIterator.class);
      BlobStoreFilterOutJsonFieldsIterator.setFieldsToKeep(setting, fields);

      scanner.addScanIterator(setting);
    }

    IteratorSetting setting =
        new IteratorSetting(MASKING_ITERATOR_PRIORITY, BlobStoreMaskingIterator.class);
    BlobStoreMaskingIterator.setAuthorizations(setting, scanner.getAuthorizations());
    // TODO : set salt

    scanner.addScanIterator(setting);

    if (hashes != null && !hashes.isEmpty()) {

      IteratorSetting settings = new IteratorSetting(MASKED_JSON_FIELD_FILTER_ITERATOR_PRIORITY,
          BlobStoreMaskedJsonFieldFilter.class);
      hashes.forEach(
          e -> BlobStoreMaskedJsonFieldFilter.addFilter(settings, e.getKey(), e.getValue()));

      scanner.addScanIterator(settings);
    }

    List<Range> ranges;

    if (keys == null || keys.isEmpty()) {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    } else {
      ranges = Range.mergeOverlapping(
          keys.stream().map(key -> Range.exact(new Text(dataset + SEPARATOR_NUL + key)))
              .collect(Collectors.toList()));
    }

    if (!setRanges(scanner, ranges)) {
      return View.of();
    }

    View<Map.Entry<Key, Value>> view;

    if (scanner instanceof BatchScanner) {
      view = new UnorderedView<>((BatchScanner) scanner, s -> s.iterator());
    } else {
      view = new OrderedView<>((Scanner) scanner, s -> s.iterator());
    }
    return view.map(entry -> Blob.fromKeyValue(entry.getKey(), entry.getValue()));
  }
}
