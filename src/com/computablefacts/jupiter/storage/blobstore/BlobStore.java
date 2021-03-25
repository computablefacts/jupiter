package com.computablefacts.jupiter.storage.blobstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.iterators.BlobStoreAnonymizingIterator;
import com.computablefacts.jupiter.iterators.BlobStoreFilterOutJsonFieldsIterator;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * This storage layer acts as a blob store. The storage layer utilizes the
 * <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as the basis for
 * its ingest and query components.
 * </p>
 * 
 * <pre>
 *  Row         | Column Family | Column Qualifier                                | Visibility                             |Value
 * =============+===============+=================================================+========================================+========
 *  <key>       | <dataset>     | <blob_type>\0<property_1>\0<property_2>\0...    | ADM|<dataset>_RAW_DATA|<dataset>_<key> | <blob>
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class BlobStore extends AbstractStorage {

  private static final Logger logger_ = LoggerFactory.getLogger(BlobStore.class);

  public BlobStore(Configurations configurations, String name) {
    super(configurations, name);
  }

  /**
   * Group data belonging to a same dataset together.
   *
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean addLocalityGroup(String dataset) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("add_locality_group", tableName())
          .add("dataset", dataset).formatInfo());
    }

    Map<String, Set<Text>> groups =
        Tables.getLocalityGroups(configurations().tableOperations(), tableName());

    if (!groups.containsKey(dataset)) {

      groups.put(dataset, Sets.newHashSet(new Text(dataset)));

      return Tables.setLocalityGroups(configurations().tableOperations(), tableName(), groups,
          false);
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
    return remove(deleter, null, dataset, null);
  }

  /**
   * Remove keys for a given dataset.
   *
   * @param deleter batch deleter.
   * @param dataset dataset.
   * @param keys a set of keys to remove.
   * @return true if the operation succeeded, false otherwise.
   */
  @Beta
  public boolean removeKeys(BatchDeleter deleter, String dataset, Set<String> keys) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("keys", keys).formatDebug());
    }

    @Var
    int nbRemovedKeys = 0;

    for (String key : keys) {
      if (remove(deleter, key, dataset, null)) {
        nbRemovedKeys++;
      }
    }
    return nbRemovedKeys == keys.size();
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

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(file, "file should not be null");
    Preconditions.checkArgument(file.exists(), "Missing file : %s", file);

    List<String> properties = Lists.newArrayList(file.getName(), Long.toString(file.length(), 10));

    try {
      byte[] content = java.nio.file.Files.readAllBytes(file.toPath());
      return put(writer, dataset, key, labels, Blob.TYPE_FILE, properties, content);
    } catch (IOException e) {
      logger_.error(
          LogFormatterManager.logFormatter().add("table_name", tableName()).add("dataset", dataset)
              .add("key", key).add("file", file).add("labels", labels).message(e).formatError());
    }
    return false;
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

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return put(writer, dataset, key, labels, Blob.TYPE_STRING, null,
        value.getBytes(StandardCharsets.UTF_8));
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

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return put(writer, dataset, key, labels, Blob.TYPE_JSON, null,
        value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get all blobs. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> get(ScannerBase scanner, String dataset) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return get(scanner, dataset, null, Sets.newHashSet());
  }

  /**
   * Get a single blob.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @param key key.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> get(ScannerBase scanner, String dataset, String key) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");

    return get(scanner, dataset, null, Sets.newHashSet(key));
  }

  /**
   * Get one or more blobs.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @param keepFields fields to keep if Accumulo Values are JSON objects (optional).
   * @param keys keys.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<Blob<Value>> get(ScannerBase scanner, String dataset, Set<String> keepFields,
      Set<String> keys) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(keys, "keys should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("has_keep_fields", keepFields != null)
          .add("keys.size", keys.size()).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(dataset));

    IteratorSetting setting = new IteratorSetting(21, BlobStoreAnonymizingIterator.class);
    BlobStoreAnonymizingIterator.setAuthorizations(setting, scanner.getAuthorizations());

    scanner.addScanIterator(setting);

    if (keepFields != null) {

      IteratorSetting setting2 =
          new IteratorSetting(22, BlobStoreFilterOutJsonFieldsIterator.class);
      BlobStoreFilterOutJsonFieldsIterator.setAuthorizations(setting2, scanner.getAuthorizations());
      BlobStoreFilterOutJsonFieldsIterator.setFieldsToKeep(setting2, keepFields);

      scanner.addScanIterator(setting2);
    }

    List<Range> ranges;

    if (keys.isEmpty()) {
      ranges = Lists.newArrayList(new Range());
    } else {
      ranges = Range.mergeOverlapping(keys.stream()
          .map(key -> Range.exact(new Text(key), new Text(dataset))).collect(Collectors.toList()));
    }

    if (!setRanges(scanner, ranges)) {
      return Constants.ITERATOR_EMPTY;
    }
    return Iterators.transform(scanner.iterator(), entry -> {

      String cv = entry.getKey().getColumnVisibility().toString();
      Set<String> labels = Sets.newHashSet(
          Splitter.on(Constants.SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));
      String cq = entry.getKey().getColumnQualifier().toString();

      int index = cq.indexOf(Constants.SEPARATOR_NUL);
      if (index < 0) {
        return new Blob<>(entry.getKey().getRow().toString(), labels, Blob.TYPE_UNKNOWN,
            Lists.newArrayList(), entry.getValue());
      }

      List<String> properties =
          Splitter.on(Constants.SEPARATOR_NUL).trimResults().omitEmptyStrings().splitToList(cq);

      return new Blob<>(entry.getKey().getRow().toString(), labels,
          Integer.parseInt(properties.get(0), 10), properties.subList(1, properties.size()),
          entry.getValue());
    });
  }

  /**
   * Persist a blob.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param type type of blob in {UNKNOWN, STRING, FILE}.
   * @param properties list of properties.
   * @param bytes blob.
   * @return true if the operation succeeded, false otherwise.
   */
  private boolean put(BatchWriter writer, String dataset, String key, Set<String> labels, int type,
      List<String> properties, byte[] bytes) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkArgument(type >= 0, "unknown blob type : %s", type);
    Preconditions.checkNotNull(bytes, "bytes should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("key", key).add("labels", labels).add("type", type)
          .add("properties", properties).formatDebug());
    }

    StringBuilder cq = new StringBuilder();

    cq.append(type);
    cq.append(Constants.SEPARATOR_NUL);

    if (properties != null && !properties.isEmpty()) {
      cq.append(Joiner.on(Constants.SEPARATOR_NUL).join(properties));
    }

    ColumnVisibility viz = new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE).join(labels));
    return add(writer, new Text(key), new Text(dataset), new Text(cq.toString()), viz,
        new Value(bytes));
  }
}
