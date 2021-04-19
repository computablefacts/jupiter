package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.TEXT_CACHE;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class DataStoreCache {

  private static final Logger logger_ = LoggerFactory.getLogger(DataStoreCache.class);
  private static final ExecutorService executorService_ = Executors.newFixedThreadPool(3);

  private DataStoreCache() {}

  /**
   * Check if a cache id already exists.
   *
   * @param scanners scanners.
   * @param cacheId the cache id.
   * @return true iif the cache id already exists, false otherwise.
   */
  public static boolean hasData(Scanners scanners, String cacheId) {
    return read(scanners, cacheId).hasNext();
  }

  /**
   * Get a list of values.
   *
   * @param scanners scanners.
   * @param cacheId the cache id.
   * @return a list of values.
   */
  public static Iterator<String> read(Scanners scanners, String cacheId) {
    return read(scanners, cacheId, null);
  }

  /**
   * Get cached values.
   *
   * @param scanners scanners.
   * @param cacheId the cache id.
   * @param nextValue where to start iterating.
   * @return a list of values.
   */
  public static Iterator<String> read(Scanners scanners, String cacheId, String nextValue) {

    Preconditions.checkNotNull(scanners, "scanners should neither be null nor empty");
    Preconditions.checkNotNull(cacheId, "cacheId should neither be null nor empty");

    scanners.blob().clearColumns();
    scanners.blob().clearScanIterators();
    scanners.blob().fetchColumnFamily(TEXT_CACHE);

    Range range;

    if (nextValue == null) {
      range = Range.exact(new Text(cacheId), TEXT_CACHE);
    } else {
      Key begin = new Key(new Text(cacheId), TEXT_CACHE, new Text(nextValue));
      Key end = begin.followingKey(PartialKey.ROW);
      range = new Range(begin, true, end, false);
    }

    if (!AbstractStorage.setRange(scanners.blob(), range)) {
      return ITERATOR_EMPTY;
    }
    return Iterators.transform(scanners.blob().iterator(),
        entry -> entry.getKey().getColumnQualifier().toString());
  }

  /**
   * Cache values.
   *
   * @param scanners scanners (optional).
   * @param writers writers.
   * @param cacheId the cache id.
   * @param iterator the values to cache.
   */
  public static void write(Scanners scanners, Writers writers, String cacheId,
      Iterator<String> iterator) {
    write(scanners, writers, cacheId, iterator, -1);
  }

  /**
   * Cache values.
   *
   * @param scanners scanners (optional).
   * @param writers writers.
   * @param cacheId the cache id.
   * @param iterator the values to cache.
   * @param delegateToBackgroundThreadAfter synchronously write to cache until this number of
   *        elements is reached. After that, delegate the remaining writes to a background thread.
   *        If this number is less than or equals to zero, performs the whole operation
   *        synchronously.
   */
  public static void write(Scanners scanners, Writers writers, String cacheId,
      Iterator<String> iterator, @Var int delegateToBackgroundThreadAfter) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(cacheId, "cacheId should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    if (delegateToBackgroundThreadAfter <= 0) {
      writeCache(scanners, writers, cacheId, iterator, -1);
    } else {
      writeCache(scanners, writers, cacheId, iterator, delegateToBackgroundThreadAfter);
      executorService_.execute(() -> writeCache(scanners, writers, cacheId, iterator, -1));
    }
  }

  private static void writeCache(Scanners scanners, Writers writers, String cacheId,
      Iterator<String> iterator, @Var int maxElementsToWrite) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(cacheId, "cacheId should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    @Var
    long nbElementsWritten = 0L;

    try {
      if (maxElementsToWrite < 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(cacheId);
          mutation.put(TEXT_CACHE, new Text(Strings.nullToEmpty(value)), VALUE_EMPTY);

          writers.blob().addMutation(mutation);
          nbElementsWritten++;
        }
      } else if (maxElementsToWrite > 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(cacheId);
          mutation.put(TEXT_CACHE, new Text(Strings.nullToEmpty(value)), VALUE_EMPTY);

          writers.blob().addMutation(mutation);
          nbElementsWritten++;

          if (--maxElementsToWrite <= 0) {
            break;
          }
        }
      } else {
        logger_.warn(
            LogFormatterManager.logFormatter().message("write ignored").add("cache_id", cacheId)
                .add("max_elements_to_write", maxElementsToWrite).formatWarn());
      }

      if (logger_.isDebugEnabled()) {
        logger_.debug(LogFormatterManager.logFormatter().add("cache_id", cacheId)
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
          logger_.error(LogFormatterManager.logFormatter().message("flush failed")
              .add("cache_id", cacheId).add("max_elements_to_write", maxElementsToWrite)
              .add("nb_elements_written", nbElementsWritten).formatError());
          break;
        }
        maxTries--;
      } while (scanners != null && maxTries > 0 && nbElementsWritten > 0
          && !hasData(scanners, cacheId));

    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    }
  }
}
