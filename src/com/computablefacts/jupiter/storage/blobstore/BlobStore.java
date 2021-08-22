package com.computablefacts.jupiter.storage.blobstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.combiners.BlobStoreCombiner;
import com.computablefacts.jupiter.combiners.DataStoreHashIndexCombiner;
import com.computablefacts.jupiter.iterators.BlobStoreFilterOutJsonFieldsIterator;
import com.computablefacts.jupiter.iterators.BlobStoreMaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

  private static final Logger logger_ = LoggerFactory.getLogger(BlobStore.class);

  public BlobStore(Configurations configurations, String name) {
    super(configurations, name);
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

      // Remove legacy iterators from the BlobStore
      Map<String, EnumSet<IteratorUtil.IteratorScope>> iterators =
          configurations().tableOperations().listIterators(tableName());

      if (iterators.containsKey("BlobStoreCombiner")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(),
            BlobStoreCombiner.class.getSimpleName(), EnumSet.of(IteratorUtil.IteratorScope.majc,
                IteratorUtil.IteratorScope.minc, IteratorUtil.IteratorScope.scan));
      }

      if (iterators.containsKey("DataStoreHashIndexCombiner")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(),
            DataStoreHashIndexCombiner.class.getSimpleName(),
            EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc,
                IteratorUtil.IteratorScope.scan));
      }

      // Set the array combiner
      IteratorSetting settings = new IteratorSetting(7, BlobStoreCombiner.class);
      BlobStoreCombiner.setColumns(settings,
          Lists.newArrayList(new IteratorSetting.Column(TYPE_ARRAY)));
      BlobStoreCombiner.setReduceOnFullCompactionOnly(settings, true);

      configurations().tableOperations().attachIterator(tableName(), settings);

      // Set locality groups
      return addLocalityGroups(Sets.newHashSet(TYPE_STRING, TYPE_FILE, TYPE_JSON, TYPE_ARRAY));

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
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

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .formatDebug());
    }
    return removeRanges(deleter, Sets.newHashSet(Range.prefix(dataset + SEPARATOR_NUL)));
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
   * Get all blobs. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> getStrings(ScannerBase scanner, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(scanner, dataset, TYPE_STRING, keys, fields);
  }

  /**
   * Get all blobs. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> getJsons(ScannerBase scanner, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(scanner, dataset, TYPE_JSON, keys, fields);
  }

  /**
   * Get all blobs. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> getFiles(ScannerBase scanner, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(scanner, dataset, TYPE_FILE, keys, fields);
  }

  /**
   * Get all blobs. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> getArrays(ScannerBase scanner, String dataset, Set<String> keys,
      Set<String> fields) {
    return get(scanner, dataset, TYPE_ARRAY, keys, fields);
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
   * @return an iterator of (key, value) pairs.
   */
  private Iterator<Blob<Value>> get(ScannerBase scanner, String dataset, String blobType,
      Set<String> keys, Set<String> fields) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(blobType, "blobType should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("dataset", dataset)
          .add("blob_type", blobType).add("has_fields", fields != null)
          .add("has_keys", keys != null).formatDebug());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(blobType));

    @Var
    int priority = 21;

    if (fields != null && !fields.isEmpty()) {

      IteratorSetting setting =
          new IteratorSetting(priority++, BlobStoreFilterOutJsonFieldsIterator.class);
      BlobStoreFilterOutJsonFieldsIterator.setFieldsToKeep(setting, fields);

      scanner.addScanIterator(setting);
    }

    IteratorSetting setting = new IteratorSetting(priority, BlobStoreMaskingIterator.class);
    BlobStoreMaskingIterator.setAuthorizations(setting, scanner.getAuthorizations());
    // TODO : set salt

    scanner.addScanIterator(setting);

    List<Range> ranges;

    if (keys == null || keys.isEmpty()) {
      ranges = Lists.newArrayList(Range.prefix(dataset + SEPARATOR_NUL));
    } else {
      ranges = Range.mergeOverlapping(
          keys.stream().map(key -> Range.exact(new Text(dataset + SEPARATOR_NUL + key)))
              .collect(Collectors.toList()));
    }

    if (!setRanges(scanner, ranges)) {
      return Constants.ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(),
        entry -> Blob.fromKeyValue(entry.getKey(), entry.getValue()));
  }
}
