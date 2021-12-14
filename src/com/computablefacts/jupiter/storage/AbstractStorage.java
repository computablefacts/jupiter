package com.computablefacts.jupiter.storage;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.asterix.Generated;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
public abstract class AbstractStorage {

  private static final Logger logger_ = LoggerFactory.getLogger(AbstractStorage.class);

  private final Configurations configurations_;
  private final String tableName_;

  public AbstractStorage(Configurations configurations, String tableName) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    configurations_ = configurations;
    tableName_ = tableName;
  }

  @Deprecated
  public static String encode(String string) {

    Preconditions.checkNotNull(string, "string should not be null");

    return string.replace("\0", "\\u0000");
  }

  @Deprecated
  public static String decode(String string) {

    Preconditions.checkNotNull(string, "string should not be null");

    return string.replace("\\u0000", "\0");
  }

  /**
   * Normalize a random string to match Accumulo visibility labels.
   *
   * @param string string.
   * @return visibility label.
   */
  public static String toVisibilityLabel(String string) {

    Preconditions.checkNotNull(string, "string should not be null");

    return string.replaceAll("\\[\\d+\\]|\\[\\*\\]", "").replaceAll("\\s+|[^a-zA-Z0-9_]", "_")
        .trim().toUpperCase();
  }

  /**
   * Normalize the first 3 levels of a path to match Accumulo visibility labels.
   *
   * @param path path.
   * @return visibility labels.
   */
  public static Set<String> toVisibilityLabels(List<String> path) {

    Preconditions.checkNotNull(path, "path should not be null");

    Set<String> set = new HashSet<>();

    for (int i = 0; i < 3 && i < path.size(); i++) {
      set.add(toVisibilityLabel(Joiner.on("_").join(path.subList(0, i + 1))));
    }
    return set;
  }

  /**
   * Return an empty set of authorization if authorizations is null.
   *
   * @param authorizations authorizations.
   * @return authorizations, not null.
   */
  public static Authorizations nullToEmpty(Authorizations authorizations) {
    return authorizations == null ? Authorizations.EMPTY : authorizations;
  }

  /**
   * Keep only authorizations starting with {@code <dataset>_}.
   *
   * @param authorizations authorizations.
   * @param dataset dataset.
   * @return authorizations, not null.
   */
  public static Authorizations compact(Authorizations authorizations, String dataset) {

    if (authorizations == null) {
      return Authorizations.EMPTY;
    }
    if (Strings.isNullOrEmpty(dataset)) {
      return authorizations;
    }

    String label = toVisibilityLabel(dataset + "_");
    Set<String> auths =
        Splitter.on(',').trimResults().omitEmptyStrings().splitToStream(authorizations.toString())
            .filter(auth -> auth.startsWith(label)).collect(Collectors.toSet());

    return new Authorizations(Iterables.toArray(auths, String.class));
  }

  /**
   * Set a single range to a scanner.
   *
   * @param scanner scanner.
   * @param range a single range.
   * @return true if the action succeeded, false otherwise.
   */
  public static boolean setRange(ScannerBase scanner, Range range) {
    return setRanges(scanner, Sets.newHashSet(range));
  }

  /**
   * Set a collection of ranges to a scanner.
   *
   * @param scanner scanner.
   * @param ranges a set of ranges.
   * @return true if the action succeeded, false otherwise.
   */
  public static boolean setRanges(ScannerBase scanner, Collection<Range> ranges) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(ranges, "ranges should not be null");

    if (scanner instanceof BatchScanner) {
      ((BatchScanner) scanner).setRanges(ranges);
    } else if (scanner instanceof Scanner) {
      if (ranges.size() == 1) {
        ((Scanner) scanner).setRange((Range) ranges.toArray()[0]);
      } else {
        logger_.error(LogFormatter.create(true)
            .message("\"using a BatchScanner is mandatory : the number of ranges is > 1\"")
            .formatError());
        return false;
      }
    } else {
      logger_.error(LogFormatter.create(true).message("\"invalid scanner type\"").formatError());
      return false;
    }
    return true;
  }

  /**
   * This method differentiates between various types of exceptions we may see.
   *
   * @param exception the exception to analyze.
   * @return true if we could retry, false if we should exit.
   */
  @CanIgnoreReturnValue
  protected static boolean handleExceptions(Exception exception) {

    Preconditions.checkNotNull(exception, "exception should not be null");

    logger_.error(LogFormatter.create(true).message(exception).formatError());

    if (exception instanceof MutationsRejectedException) {

      // Permanent failures
      MutationsRejectedException ex = (MutationsRejectedException) exception;
      Map<TabletId, Set<SecurityErrorCode>> securityErrors = ex.getSecurityErrorCodes();

      for (Map.Entry<TabletId, Set<SecurityErrorCode>> entry : securityErrors.entrySet()) {
        for (SecurityErrorCode err : entry.getValue()) {
          logger_.error(LogFormatter.create(true).message("Permanent error: " + err.toString())
              .formatError());
        }
      }

      List<ConstraintViolationSummary> constraintViolations = ex.getConstraintViolationSummaries();

      for (ConstraintViolationSummary cvs : constraintViolations) {
        logger_.error(LogFormatter.create(true).message("Constraint violation: " + cvs.toString())
            .formatError());
      }

      if (!securityErrors.isEmpty() || !constraintViolations.isEmpty()) {
        logger_.error(
            LogFormatter.create(true).message("Have permanent errors. Exiting...").formatError());
        return false;
      }

      // Transient failures
      Collection<String> errorServers = ex.getErrorServers();

      for (String errorServer : errorServers) {
        logger_.warn(
            LogFormatter.create(true).message("Problem with server: " + errorServer).formatWarn());
      }

      int numUnknownExceptions = ex.getUnknownExceptions();

      if (numUnknownExceptions > 0) {
        logger_.warn(LogFormatter.create(true)
            .message(numUnknownExceptions + " unknown exceptions.").formatWarn());
      }
      return true;
    } else if (exception instanceof TimedOutException) {

      // Transient failures
      TimedOutException ex = (TimedOutException) exception;
      Collection<String> errorServers = ex.getTimedOutSevers();

      for (String errorServer : errorServers) {
        logger_.warn(LogFormatter.create(true)
            .message("Problem with server: " + errorServer + " (timeout)").formatWarn());
      }
      return true;
    }
    return false;
  }

  /**
   * Get the cluster configuration.
   *
   * @return the cluster configuration.
   */
  @Generated
  public Configurations configurations() {
    return configurations_;
  }

  /**
   * Get the table name.
   *
   * @return table name.
   */
  @Generated
  public String tableName() {
    return tableName_;
  }

  /**
   * Group data belonging to a same column family together.
   *
   * @param cfs column families.
   * @return true if the operation succeeded, false otherwise.
   */
  @CanIgnoreReturnValue
  public boolean addLocalityGroups(Set<String> cfs) {

    Preconditions.checkNotNull(cfs, "cfs should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("table_name", tableName()).add("cfs", cfs).formatDebug());
    }

    if (!cfs.isEmpty()) {

      Map<String, Set<Text>> groups =
          Tables.getLocalityGroups(configurations().tableOperations(), tableName());

      cfs.stream().filter(cf -> !groups.containsKey(cf))
          .forEach(cf -> groups.put(cf, Sets.newHashSet(new Text(cf))));

      return Tables.setLocalityGroups(configurations().tableOperations(), tableName(), groups,
          false);
    }
    return true;
  }

  /**
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    return Tables.exists(configurations().tableOperations(), tableName());
  }

  /**
   * Initialize the storage layer.
   *
   * @return true if the storage layer already exists or has been successfully initialized, false
   *         otherwise.
   */
  public boolean create() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    if (!isReady()) {

      @Var
      boolean isOk = Tables.create(configurations().tableOperations(), tableName());
      isOk = isOk && Tables.dataBlockCache(configurations().tableOperations(), tableName(), true);
      isOk = isOk && Tables.dataIndexCache(configurations().tableOperations(), tableName(), true);

      return isOk;
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

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    if (isReady()) {
      return Tables.delete(configurations().tableOperations(), tableName());
    }
    return true;
  }

  /**
   * Remove all data from the table. Existing splits are kept.
   *
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean truncate() {

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).formatDebug());
    }

    SortedSet<Text> splits = Tables.splits(configurations().tableOperations(), tableName());

    if (Tables.deleteAllRows(configurations().tableOperations(), tableName())) {
      return Tables.split(configurations().tableOperations(), tableName(), splits);
    }
    return true;
  }

  /**
   * Remove a set of column families.
   *
   * @param deleter batch deleter.
   * @param cfs the column families to remove.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeColumnFamilies(BatchDeleter deleter, Set<String> cfs) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(cfs, "cfs should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("table_name", tableName()).add("cfs", cfs).formatDebug());
    }

    deleter.clearColumns();
    deleter.clearScanIterators();

    try {
      deleter.setRanges(Collections.singleton(new Range()));
      for (String cf : cfs) {
        deleter.fetchColumnFamily(new Text(cf));
      }
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      handleExceptions(e);
      return false;
    }
    return true;
  }

  /**
   * Remove a set of ranges.
   *
   * @param deleter batch deleter.
   * @param ranges the ranges to remove.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean removeRanges(BatchDeleter deleter, Set<Range> ranges) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(ranges, "ranges should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("ranges", ranges)
          .formatDebug());
    }

    deleter.clearColumns();
    deleter.clearScanIterators();

    try {
      deleter.setRanges(ranges);
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      handleExceptions(e);
      return false;
    }
    return true;
  }

  /**
   * Remove data.
   *
   * @param deleter batch deleter.
   * @param row the row to delete. If null, delete all data in the [*, cf, cq] or [*, cf, *] ranges.
   *        If the column family and column qualifier parameters are also null, remove all data.
   * @param cf the column family to delete. If null, delete all data in the [row, *, *] range.
   * @param cq the column qualifier to delete. If null, delete all data in the [row, *, *] or [row,
   *        cf, *] ranges.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean remove(BatchDeleter deleter, String row, String cf, String cq) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("table_name", tableName()).add("row", row)
          .add("cf", cf).add("cq", cq).formatDebug());
    }

    deleter.clearColumns();
    deleter.clearScanIterators();

    try {
      deleter.setRanges(Collections.singleton(row == null ? new Range() : Range.exact(row)));
      if (cf != null && cq != null) {
        deleter.fetchColumn(new Text(cf), new Text(cq));
      } else if (cf != null) {
        deleter.fetchColumnFamily(new Text(cf));
      }
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      handleExceptions(e);
      return false;
    }
    return true;
  }

  /**
   * Persist data.
   *
   * @param writer batch writer.
   * @param mutation the mutation to write.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean add(BatchWriter writer, Mutation mutation) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(mutation, "mutation should not be null");

    try {
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      handleExceptions(e);
      return false;
    }
    return true;
  }

  @Deprecated
  public BatchWriter writer() {
    return writer(Tables.batchWriterConfig());
  }

  public BatchWriter writer(BatchWriterConfig config) {
    return Tables.batchWriter(configurations().connector(), tableName(), config);
  }

  public BatchDeleter deleter(Authorizations authorizations, int nbQueryThreads,
      BatchWriterConfig config) {

    Preconditions.checkArgument(nbQueryThreads > 0, "nbQueryThreads should be > 0");
    Preconditions.checkNotNull(config, "config should not be null");

    return Tables.batchDeleter(configurations().connector(), tableName(),
        nullToEmpty(authorizations), nbQueryThreads, config);
  }

  public BatchScanner batchScanner(Authorizations authorizations, int nbQueryThreads) {

    Preconditions.checkArgument(nbQueryThreads > 0, "nbQueryThreads should be > 0");

    return Tables.batchScanner(configurations().connector(), tableName(),
        nullToEmpty(authorizations), nbQueryThreads);
  }

  public Scanner scanner(Authorizations authorizations) {
    return Tables.scanner(configurations().connector(), tableName(), nullToEmpty(authorizations));
  }
}
