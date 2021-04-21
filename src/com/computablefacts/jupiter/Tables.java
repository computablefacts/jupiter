package com.computablefacts.jupiter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Tables {

  private static final Logger logger_ = LoggerFactory.getLogger(Tables.class);

  private Tables() {}

  public static SortedSet<String> all(TableOperations tableOperations) {
    return Preconditions.checkNotNull(tableOperations, "tableOperations should not be null").list();
  }

  public static boolean exists(TableOperations tableOperations, String tableName) {
    return Preconditions.checkNotNull(tableOperations, "tableOperations should not be null")
        .exists(tableName);
  }

  public static boolean create(TableOperations tableOperations, String tableName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      if (!exists(tableOperations, tableName)) {
        tableOperations.create(tableName);
      }
      return true;
    } catch (TableExistsException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean delete(TableOperations tableOperations, String tableName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      if (exists(tableOperations, tableName)) {
        tableOperations.delete(tableName);
      }
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean rename(TableOperations tableOperations, String oldName, String newName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(oldName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(newName));

    try {
      if (exists(tableOperations, oldName) && !exists(tableOperations, newName)) {
        tableOperations.rename(oldName, newName);
        return true;
      }
      return false;
    } catch (TableNotFoundException | TableExistsException | AccumuloSecurityException
        | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean clone(TableOperations tableOperations, String tableSrc, String tableDest) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableSrc));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableDest));

    try {
      if (exists(tableOperations, tableSrc) && !exists(tableOperations, tableDest)) {
        tableOperations.clone(tableSrc, tableDest, true, new HashMap<>(), new HashSet<>());
        return true;
      }
      return false;
    } catch (TableNotFoundException | TableExistsException | AccumuloSecurityException
        | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean split(TableOperations tableOperations, String tableName,
      SortedSet<Text> partitionKeys) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(partitionKeys, "partitionKeys should not be null");

    try {
      if (exists(tableOperations, tableName)) {
        tableOperations.addSplits(tableName, partitionKeys);
      }
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static SortedSet<Text> splits(TableOperations tableOperations, String tableName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      if (exists(tableOperations, tableName)) {
        return new TreeSet<>(tableOperations.listSplits(tableName));
      }
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return new TreeSet<>();
  }

  public static boolean bloomFilter(TableOperations tableOperations, String tableName,
      boolean enable) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      tableOperations.setProperty(tableName, "table.bloom.enabled", enable ? "true" : "false");
      return true;
    } catch (AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean dataBlockCache(TableOperations tableOperations, String tableName,
      boolean enable) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      tableOperations.setProperty(tableName, "table.cache.block.enable", enable ? "true" : "false");
      return true;
    } catch (AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean dataIndexCache(TableOperations tableOperations, String tableName,
      boolean enable) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      tableOperations.setProperty(tableName, "table.cache.index.enable", enable ? "true" : "false");
      return true;
    } catch (AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean setLocalityGroups(TableOperations tableOperations, String tableName,
      Map<String, Set<Text>> groups, boolean compact) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkArgument(groups != null && !groups.isEmpty());

    try {
      tableOperations.setLocalityGroups(tableName, groups);
      if (compact) {
        tableOperations.compact(tableName, null, null, false, false);
      }
      return true;
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static Map<String, Set<Text>> getLocalityGroups(TableOperations tableOperations,
      String tableName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      return tableOperations.getLocalityGroups(tableName);
    } catch (AccumuloException | TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return new HashMap<>();
  }

  public static boolean deleteAllRows(TableOperations tableOperations, String tableName) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");

    try {
      tableOperations.deleteRows(tableName, null, null);
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean deleteRowsFrom(TableOperations tableOperations, String tableName,
      String from) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(from, "from should not be null");

    try {
      tableOperations.deleteRows(tableName, new Text(from), null);
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean deleteRowsTo(TableOperations tableOperations, String tableName, String to) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(to, "to should not be null");

    try {
      tableOperations.deleteRows(tableName, null, new Text(to));
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public static boolean deleteRows(TableOperations tableOperations, String tableName, String from,
      String to) {

    Preconditions.checkNotNull(tableOperations, "tableOperations should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(from, "from should not be null");
    Preconditions.checkNotNull(to, "to should not be null");

    try {
      tableOperations.deleteRows(tableName,
          new Text(from.equals(to)
              ? from.substring(0, from.length() - 1)
                  + Character.toString((char) (from.charAt(from.length() - 1) - 1))
              : from),
          new Text(to));
      return true;
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  @Deprecated
  public static BatchWriterConfig batchWriterConfig() {

    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();

    // See https://accumulo.apache.org/blog/2016/11/02/durability-performance.html
    batchWriterConfig.setDurability(Durability.FLUSH);
    return batchWriterConfig;
  }

  public static BatchWriterConfig batchWriterConfig(long maxMemoryInBytes, long maxLatencyInMs,
      int maxWriteThreads, long timeoutInMs) {

    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    batchWriterConfig.setMaxMemory(maxMemoryInBytes);
    batchWriterConfig.setMaxLatency(maxLatencyInMs, TimeUnit.MILLISECONDS);
    batchWriterConfig.setMaxWriteThreads(maxWriteThreads);
    batchWriterConfig.setTimeout(timeoutInMs, TimeUnit.MILLISECONDS);

    // See https://accumulo.apache.org/blog/2016/11/02/durability-performance.html
    batchWriterConfig.setDurability(Durability.FLUSH);
    return batchWriterConfig;
  }

  public static Scanner scanner(Connector connector, String tableName,
      Authorizations authorizations) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");

    try {
      return connector.createScanner(tableName, authorizations);
    } catch (TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return null;
  }

  public static BatchScanner batchScanner(Connector connector, String tableName,
      Authorizations authorizations, int nbQueryThreads) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkArgument(nbQueryThreads > 0, "nbQueryThreads should be > 0");

    try {
      return connector.createBatchScanner(tableName, authorizations, nbQueryThreads);
    } catch (TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return null;
  }

  public static BatchDeleter batchDeleter(Connector connector, String tableName,
      Authorizations authorizations, int nbQueryThreads, BatchWriterConfig batchWriterConfig) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkArgument(nbQueryThreads > 0, "nbQueryThreads should be > 0");
    Preconditions.checkNotNull(batchWriterConfig, "batchWriterConfig should not be null");

    try {
      return connector.createBatchDeleter(tableName, authorizations, nbQueryThreads,
          batchWriterConfig);
    } catch (TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return null;
  }

  public static MultiTableBatchWriter multiTableBatchWriter(Connector connector,
      BatchWriterConfig batchWriterConfig) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkNotNull(batchWriterConfig, "batchWriterConfig should not be null");

    return connector.createMultiTableBatchWriter(batchWriterConfig);
  }

  public static BatchWriter batchWriter(Connector connector, String tableName,
      BatchWriterConfig batchWriterConfig) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
        "tableName should neither be null nor empty");
    Preconditions.checkNotNull(batchWriterConfig, "batchWriterConfig should not be null");

    try {
      return connector.createBatchWriter(tableName, batchWriterConfig);
    } catch (TableNotFoundException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return null;
  }
}
