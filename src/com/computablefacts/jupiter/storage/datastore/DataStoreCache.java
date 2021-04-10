package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.TEXT_CACHE;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.Iterator;
import java.util.UUID;
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
   * Get a list of values.
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
      range = Range.exact(cacheId);
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
   * Write a list of values.
   *
   * @param writers writers.
   * @param iterator values.
   * @return a cache id.
   */
  public static String write(Writers writers, Iterator<String> iterator) {
    return write(writers, iterator, -1);
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
  public static String write(Writers writers, Iterator<String> iterator,
      @Var int delegateToBackgroundThreadAfter) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");

    Text uuid = new Text(UUID.randomUUID().toString());

    if (delegateToBackgroundThreadAfter <= 0) {
      write(writers, iterator, uuid, -1);
    } else {
      write(writers, iterator, uuid, delegateToBackgroundThreadAfter);
      executorService_
          .execute(() -> write(writers, iterator, uuid, delegateToBackgroundThreadAfter));
    }
    return uuid.toString();
  }

  private static void write(Writers writers, Iterator<String> iterator, Text uuid,
      @Var int maxElementsToWrite) {

    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkNotNull(iterator, "iterator should not be null");
    Preconditions.checkNotNull(uuid, "uuid should not be null");

    try {
      if (maxElementsToWrite < 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(uuid);
          mutation.put(TEXT_CACHE, new Text(Strings.nullToEmpty(value)), VALUE_EMPTY);

          writers.blob().addMutation(mutation);
        }
      } else if (maxElementsToWrite > 0) {
        while (iterator.hasNext()) {

          String value = iterator.next();

          Mutation mutation = new Mutation(uuid);
          mutation.put(TEXT_CACHE, new Text(Strings.nullToEmpty(value)), VALUE_EMPTY);

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
}
