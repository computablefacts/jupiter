package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.TEXT_HASH_INDEX;

import java.util.Collections;
import java.util.Iterator;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.filters.WildcardFilter;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.FlattenIterator;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * <p>
 * This class contains helper functions to read/write fields and value hashes to the
 * {@link DataStore}.
 * </p>
 *
 * <p>
 * The underlying {@link com.computablefacts.jupiter.storage.blobstore.BlobStore} utilizes the
 * <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as the basis for
 * its ingest and query components.
 * </p>
 *
 * <pre>
 *  Row                          | Column Family          | Column Qualifier       | Visibility             | Value
 * ==============================+========================+========================+========================+========================
 *  <hash>\0<field>\0<dataset>   | hidx                   | (empty)                | (empty)                | <key1>\0<key2>\0...
 * </pre>
 * 
 */
@CheckReturnValue
final public class DataStoreHashIndex {

  private static final Logger logger_ = LoggerFactory.getLogger(DataStoreHashIndex.class);

  private DataStoreHashIndex() {}

  /**
   * Remove hashes for a given dataset.
   *
   * @param deleter deleter.
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public static boolean remove(BatchDeleter deleter, String dataset) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    deleter.clearColumns();
    deleter.clearScanIterators();
    deleter.fetchColumn(TEXT_HASH_INDEX, TEXT_EMPTY);
    deleter.setRanges(Collections.singleton(new Range()));

    IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
    WildcardFilter.applyOnRow(setting);
    WildcardFilter.addWildcard(setting, "*" + SEPARATOR_NUL + dataset);

    deleter.addScanIterator(setting);

    try {
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
      return false;
    }
    return true;
  }

  /**
   * Get documents ids.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param field the field.
   * @param value the field value.
   * @return an unordered set of docs ids.
   */
  public static Iterator<String> read(Scanners scanners, String dataset, String field,
      String value) {

    Preconditions.checkNotNull(scanners, "scanners should neither be null nor empty");
    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("dataset", dataset).add("field", field)
          .add("value", value).add("hash", DataStore.hash(value)).formatDebug());
    }

    scanners.blob().clearColumns();
    scanners.blob().clearScanIterators();
    scanners.blob().fetchColumn(TEXT_HASH_INDEX, TEXT_EMPTY);

    Range range;

    if (value != null && field != null) {
      range = Range.exact(
          new Text(DataStore.hash(value) + SEPARATOR_NUL + field + SEPARATOR_NUL + dataset),
          TEXT_HASH_INDEX, TEXT_EMPTY);
    } else if (value != null) {

      range = Range.prefix(DataStore.hash(value) + SEPARATOR_NUL);

      IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, "*" + SEPARATOR_NUL + dataset);

      scanners.blob().addScanIterator(setting);
    } else if (field != null) {

      range = new Range();

      IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, "*" + SEPARATOR_NUL + field + SEPARATOR_NUL + dataset);

      scanners.blob().addScanIterator(setting);
    } else {

      range = new Range();

      IteratorSetting setting = new IteratorSetting(21, "WildcardFilter", WildcardFilter.class);
      WildcardFilter.applyOnRow(setting);
      WildcardFilter.addWildcard(setting, "*" + SEPARATOR_NUL + dataset);

      scanners.blob().addScanIterator(setting);
    }

    if (!AbstractStorage.setRange(scanners.blob(), range)) {
      return ITERATOR_EMPTY;
    }
    return new FlattenIterator<>(scanners.blob().iterator(), entry -> {
      Value val = entry.getValue();
      return Splitter.on(SEPARATOR_NUL).trimResults().omitEmptyStrings()
          .splitToList(val.toString());
    });
  }

  /**
   * Cache fields and values.
   *
   * @param writers writers.
   * @param dataset dataset.
   * @param field the field.
   * @param value the field value.
   * @param docId the document id.
   */
  public static void write(Writers writers, String dataset, String field, String value,
      String docId) {

    Preconditions.checkNotNull(writers, "writers should neither be null nor empty");
    Preconditions.checkNotNull(field, "field should neither be null nor empty");
    Preconditions.checkNotNull(value, "value should neither be null nor empty");
    Preconditions.checkNotNull(docId, "docId should neither be null nor empty");

    Mutation mutation =
        new Mutation(DataStore.hash(value) + SEPARATOR_NUL + field + SEPARATOR_NUL + dataset);
    mutation.put(TEXT_HASH_INDEX, TEXT_EMPTY, new Value(docId));

    try {
      writers.blob().addMutation(mutation);
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
  }
}
