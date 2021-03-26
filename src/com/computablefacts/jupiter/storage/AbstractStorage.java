package com.computablefacts.jupiter.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.nona.Generated;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
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

  public static String encode(String string) {

    Preconditions.checkNotNull(string, "string should not be null");

    return string.replace("\0", "\\u0000");
  }

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

    return string.replaceAll("\\s+|[^a-zA-Z0-9_]", "_").trim().toUpperCase();
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
        logger_.error(LogFormatterManager.logFormatter()
            .message("\"using a BatchScanner is mandatory : the number of ranges is > 1\"")
            .formatError());
        return false;
      }
    } else {
      logger_.error(
          LogFormatterManager.logFormatter().message("\"invalid scanner type\"").formatError());
      return false;
    }
    return true;
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
   * Check if the storage layer has been initialized.
   *
   * @return true if the storage layer is ready to be used, false otherwise.
   */
  public boolean isReady() {

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
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

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
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

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
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

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).formatInfo());
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
  public boolean remove(BatchDeleter deleter, Set<String> cfs) {

    Preconditions.checkNotNull(deleter, "deleter should not be null");
    Preconditions.checkNotNull(cfs, "cfs should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).add("cfs", cfs)
          .formatInfo());
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
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName()).add("row", row)
          .add("cf", cf).add("cq", cq).formatInfo());
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
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
      return false;
    }
    return true;
  }

  /**
   * Persist data.
   *
   * @param writer batch writer.
   * @param row row id.
   * @param cf column family.
   * @param cq column qualifier.
   * @param cv visibility labels.
   * @param value value.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean add(BatchWriter writer, Text row, Text cf, Text cq, ColumnVisibility cv,
      Value value) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(row, "row should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("row", row).add("cf", cf).add("cq", cq).add("cv", cv)
          .add("value", value == null ? null : value.toString()).formatDebug());
    }

    Mutation mutation = new Mutation(row);
    mutation.put(cf == null ? Constants.TEXT_EMPTY : cf, cq == null ? Constants.TEXT_EMPTY : cq,
        cv == null ? Constants.VIZ_EMPTY : cv, value == null ? Constants.VALUE_EMPTY : value);

    try {
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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

  @Deprecated
  public BatchDeleter deleter(Authorizations authorizations) {
    return deleter(authorizations, 5, Tables.batchWriterConfig());
  }

  public BatchDeleter deleter(Authorizations authorizations, int nbQueryThreads,
      BatchWriterConfig config) {

    Preconditions.checkArgument(nbQueryThreads > 0, "nbQueryThreads should be > 0");
    Preconditions.checkNotNull(config, "config should not be null");

    return Tables.batchDeleter(configurations().connector(), tableName(),
        nullToEmpty(authorizations), nbQueryThreads, config);
  }

  @Deprecated
  public BatchScanner batchScanner(Authorizations authorizations) {
    return batchScanner(authorizations, 5);
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
