package com.computablefacts.jupiter.storage.cache;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.filters.AgeOffPeriodFilter;
import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.Generated;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * <p>
 * This class contains helper functions to read/write values to the {@link Cache} cache.
 * </p>
 *
 * <p>
 * The underlying {@link com.computablefacts.jupiter.storage.blobstore.BlobStore} utilizes the
 * <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as the basis for
 * its ingest and query components.
 * </p>
 *
 * <pre>
 *  Row                    | Column Family          | Column Qualifier       | Visibility             | Value
 * ========================+========================+========================+========================+========================
 *  <dataset>\0<uuid>      | (empty)                | <cached_string>        | (empty)                | (empty)
 * </pre>
 *
 * <p>
 * And / or :
 * </p>
 *
 * <pre>
 *  Row                    | Column Family          | Column Qualifier       | Visibility             | Value
 * ========================+========================+========================+========================+========================
 *  <dataset>\0<uuid>      | (empty)                | <hashed_string>        | (empty)                | <cached_string>
 * </pre>
 *
 */
@CheckReturnValue
public final class Cache {

  private static final Logger logger_ = LoggerFactory.getLogger(Cache.class);
  private static final ExecutorService executorService_ = Executors.newFixedThreadPool(3);

  private final BlobStore cache_;

  public Cache(Configurations configurations, String tableName) {
    cache_ = new BlobStore(configurations, tableName);
  }

  /**
   * Get the table name.
   *
   * @return table name.
   */
  @Generated
  public String tableName() {
    return cache_.tableName();
  }

  /**
   * Get the cluster configuration.
   *
   * @return the cluster configuration.
   */
  @Generated
  public Configurations configurations() {
    return cache_.configurations();
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  @Generated
  public boolean isReady() {
    return cache_.isReady();
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  @Generated
  public boolean create() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    if (!isReady()) {
      if (!cache_.create()) {
        return false;
      }
    }

    try {

      // Remove legacy iterators from the cache
      Map<String, EnumSet<IteratorUtil.IteratorScope>> iterators =
          configurations().tableOperations().listIterators(tableName());

      if (iterators.containsKey("AgeOffPeriodFilter")) { // TODO : remove after migration
        configurations().tableOperations().removeIterator(tableName(),
            AgeOffPeriodFilter.class.getSimpleName(), EnumSet.of(IteratorUtil.IteratorScope.majc,
                IteratorUtil.IteratorScope.minc, IteratorUtil.IteratorScope.scan));
      }

      // Set a 3 hours TTL on all cached data
      IteratorSetting settings = new IteratorSetting(7 + 1 /* one above the BlobStore iterator */,
          AgeOffPeriodFilter.class);
      AgeOffPeriodFilter.setTtl(settings, 3);
      AgeOffPeriodFilter.setTtlUnits(settings, "HOURS");

      configurations().tableOperations().attachIterator(tableName(), settings);

      return true;

    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  /**
   * Destroy the storage layer.
   *
   * @return true if the storage layer does not exist or has been successfully destroyed, false
   *         otherwise.
   */
  @Generated
  public boolean destroy() {
    return cache_.destroy();
  }

  /**
   * Remove all data from the table. Existing splits are kept.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  @Generated
  public boolean truncate() {
    return cache_.truncate();
  }

  @Deprecated
  public BatchDeleter deleter(Authorizations authorizations) {
    return cache_.deleter(authorizations);
  }

  /**
   * Remove cached data for a given dataset.
   *
   * @param deleter deleter.
   * @param dataset dataset.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean remove(BatchDeleter deleter, String dataset) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    deleter.clearColumns();
    deleter.clearScanIterators();
    deleter.fetchColumnFamily(TEXT_EMPTY);

    try {
      deleter.setRanges(Collections.singleton(Range.prefix(dataset + SEPARATOR_NUL)));
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
      return false;
    }
    return true;
  }

  /**
   * Check if a cache id already exists.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @return true iif the cache id already exists, false otherwise.
   */
  public boolean hasData(Scanners scanners, String dataset, String cacheId) {
    return read(scanners, dataset, cacheId).hasNext();
  }

  /**
   * Get a list of values.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @return a list of values.
   */
  public Iterator<String> read(Scanners scanners, String dataset, String cacheId) {
    return read(scanners, dataset, cacheId, null);
  }

  /**
   * Get cached values.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @param nextValue where to start iterating.
   * @return a list of values.
   */
  public Iterator<String> read(Scanners scanners, String dataset, String cacheId,
      String nextValue) {
    return read(scanners, dataset, cacheId, nextValue, false);
  }

  /**
   * Get cached values.
   *
   * @param scanners scanners.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @param nextValue where to start iterating.
   * @param isHashed if true, returns the value stored in the row value. If false, returns the value
   *        stored in the row column qualifier.
   * @return a list of values.
   */
  public Iterator<String> read(Scanners scanners, String dataset, String cacheId, String nextValue,
      boolean isHashed) {

    Preconditions.checkNotNull(scanners, "scanners should neither be null nor empty");
    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");
    Preconditions.checkNotNull(cacheId, "cacheId should neither be null nor empty");

    scanners.cache().clearColumns();
    scanners.cache().clearScanIterators();

    Range range;

    if (nextValue == null) {
      range = Range.exact(dataset + SEPARATOR_NUL + cacheId);
    } else {
      Key begin =
          new Key(new Text(dataset + SEPARATOR_NUL + cacheId), TEXT_EMPTY, new Text(nextValue));
      Key end = begin.followingKey(PartialKey.ROW);
      range = new Range(begin, true, end, false);
    }

    if (!AbstractStorage.setRange(scanners.cache(), range)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanners.cache().iterator(),
        entry -> isHashed ? entry.getValue().toString()
            : entry.getKey().getColumnQualifier().toString());
  }

  /**
   * Cache values.
   *
   * @param scanners scanners (optional).
   *
   * @param writers writers.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @param iterator the values to cache.
   */
  public void write(Scanners scanners, Writers writers, String dataset, String cacheId,
      Iterator<String> iterator) {
    write(scanners, writers, dataset, cacheId, iterator, -1);
  }

  /**
   * Cache values.
   *
   * @param scanners scanners (optional).
   * @param writers writers.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @param iterator the values to cache.
   * @param delegateToBackgroundThreadAfter synchronously write to cache until this number of
   *        elements is reached. After that, delegate the remaining writes to a background thread.
   *        If this number is less than or equals to zero, performs the whole operation
   *        synchronously.
   */
  public void write(Scanners scanners, Writers writers, String dataset, String cacheId,
      Iterator<String> iterator, @Var int delegateToBackgroundThreadAfter) {
    write(scanners, writers, dataset, cacheId, iterator, delegateToBackgroundThreadAfter, false);
  }

  /**
   * Cache values.
   *
   * @param scanners scanners (optional).
   * @param writers writers.
   * @param dataset dataset.
   * @param cacheId the cache id.
   * @param iterator the values to cache.
   * @param delegateToBackgroundThreadAfter synchronously write to cache until this number of
   *        elements is reached. After that, delegate the remaining writes to a background thread.
   *        If this number is less than or equals to zero, performs the whole operation
   *        synchronously.
   * @param hash if hash is true, the hash of the value to cache will be set in the row column
   *        qualifier and the raw value to cache will be set in the row value. If hash is false, the
   *        value to cache will be stored as-is in the row column qualifier.
   */
  public void write(Scanners scanners, Writers writers, String dataset, String cacheId,
      Iterator<String> iterator, @Var int delegateToBackgroundThreadAfter, boolean hash) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");
    Preconditions.checkNotNull(cacheId, "cacheId should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    if (delegateToBackgroundThreadAfter <= 0) {
      writeCache(scanners, writers, dataset, cacheId, iterator, -1, hash);
    } else {
      writeCache(scanners, writers, dataset, cacheId, iterator, delegateToBackgroundThreadAfter,
          hash);
      executorService_
          .execute(() -> writeCache(scanners, writers, dataset, cacheId, iterator, -1, hash));
    }
  }

  private void writeCache(Scanners scanners, Writers writers, String dataset, String cacheId,
      Iterator<String> iterator, @Var int maxElementsToWrite, boolean hash) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(cacheId, "cacheId should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    @Var
    long nbElementsWritten = 0L;

    try {
      if (maxElementsToWrite < 0) {
        while (iterator.hasNext()) {

          String value = Strings.nullToEmpty(iterator.next());
          Mutation mutation = new Mutation(dataset + SEPARATOR_NUL + cacheId);

          if (!hash) {
            mutation.put(TEXT_EMPTY, new Text(value), VALUE_EMPTY);
          } else {
            mutation.put(TEXT_EMPTY, new Text(MaskingIterator.hash(null, value)), new Value(value));
          }

          writers.cache().addMutation(mutation);
          nbElementsWritten++;
        }
      } else if (maxElementsToWrite > 0) {
        while (iterator.hasNext()) {

          String value = Strings.nullToEmpty(iterator.next());
          Mutation mutation = new Mutation(dataset + SEPARATOR_NUL + cacheId);

          if (!hash) {
            mutation.put(TEXT_EMPTY, new Text(value), VALUE_EMPTY);
          } else {
            mutation.put(TEXT_EMPTY, new Text(MaskingIterator.hash(null, value)), new Value(value));
          }

          writers.cache().addMutation(mutation);
          nbElementsWritten++;

          if (--maxElementsToWrite <= 0) {
            break;
          }
        }
      } else {
        logger_.warn(LogFormatter.create(true).message("write ignored").add("cache_id", cacheId)
            .add("max_elements_to_write", maxElementsToWrite).formatWarn());
      }

      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatter.create(true).add("cache_id", cacheId)
            .add("max_elements_to_write", maxElementsToWrite)
            .add("nb_elements_written", nbElementsWritten).formatDebug());
      }

      // Flush otherwise mutations might not have been written when read() is called. However,
      // sometimes, the first call to flush() returns but the data won't be available until a few
      // seconds later. Not OK for this particular use case!
      @Var
      int maxTries = 5;

      do {
        if (!writers.flush()) {
          logger_.error(LogFormatter.create(true).message("flush failed").add("cache_id", cacheId)
              .add("max_elements_to_write", maxElementsToWrite)
              .add("nb_elements_written", nbElementsWritten).formatError());
          break;
        }
        maxTries--;
      } while (scanners != null && maxTries > 0 && nbElementsWritten > 0
          && !hasData(scanners, dataset, cacheId));

    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
  }
}
