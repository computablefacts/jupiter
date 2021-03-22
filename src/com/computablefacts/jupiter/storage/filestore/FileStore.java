package com.computablefacts.jupiter.storage.filestore;

import java.io.IOException;
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
import com.computablefacts.jupiter.iterators.FileStoreAnonymizingIterator;
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
 * This storage layer acts as a file store. The storage layer utilizes the
 * <a href="https://accumulo.apache.org">Accumulo</a> table schemas described below as the basis for
 * its ingest and query components.
 * </p>
 * 
 * <pre>
 *  Row         | Column Family | Column Qualifier          | Visibility                             |Value
 * =============+===============+===========================+========================================+========
 *  <key>       | <dataset>     | <filename>\0<filesize>    | ADM|<dataset>_RAW_FILE|<dataset>_<key> | <blob>
 * </pre>
 *
 * <p>
 * This data store is not meant to be efficient but is intended to be easy to use.
 * </p>
 */
@CheckReturnValue
final public class FileStore extends AbstractStorage {

  private static final Logger logger_ = LoggerFactory.getLogger(FileStore.class);

  public FileStore(Configurations configurations, String name) {
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
   * @param file file.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean put(BatchWriter writer, String dataset, String key, Set<String> labels,
      java.io.File file) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(file, "file should not be null");
    Preconditions.checkArgument(file.exists(), "Missing file : %s", file);

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatterManager.logFormatter().add("table_name", tableName()).add("dataset", dataset)
              .add("key", key).add("file", file).add("labels", labels).formatDebug());
    }

    try {
      byte[] content = java.nio.file.Files.readAllBytes(file.toPath());
      return put(writer, dataset, key, labels, file.getName(), file.length(), content);
    } catch (IOException e) {
      logger_.error(
          LogFormatterManager.logFormatter().add("table_name", tableName()).add("dataset", dataset)
              .add("key", key).add("file", file).add("labels", labels).message(e).formatError());
    }
    return false;
  }

  /**
   * Persist a file.
   *
   * @param writer batch writer.
   * @param dataset dataset/namespace.
   * @param key key.
   * @param labels visibility labels.
   * @param filename filename.
   * @param fileSize the file size.
   * @param fileContent the file content as a byte array.
   * @return true if the operation succeeded, false otherwise.
   */
  public boolean put(BatchWriter writer, String dataset, String key, Set<String> labels,
      String filename, long fileSize, byte[] fileContent) {

    Preconditions.checkNotNull(writer, "writer should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(filename, "filename should not be null");
    Preconditions.checkNotNull(fileContent, "fileContent should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatterManager.logFormatter().add("table_name", tableName()).add("dataset", dataset)
              .add("key", key).add("filename", filename).add("labels", labels).formatDebug());
    }

    ColumnVisibility viz = new ColumnVisibility(Joiner.on(Constants.SEPARATOR_PIPE).join(labels));
    return add(writer, new Text(key), new Text(dataset),
        new Text(filename + Constants.SEPARATOR_NUL + fileSize), viz, new Value(fileContent));
  }

  /**
   * Get all files. Note that using a BatchScanner improves performances a lot.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<File> get(ScannerBase scanner, String dataset) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return get(scanner, dataset, Sets.newHashSet());
  }

  /**
   * Get a single file.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @param key key.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<File> get(ScannerBase scanner, String dataset, String key) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");

    return get(scanner, dataset, Sets.newHashSet(key));
  }

  /**
   * Get one or more files.
   *
   * @param scanner scanner.
   * @param dataset dataset/namespace.
   * @param keys keys.
   * @return an iterator of (key, value) pairs.
   */
  public Iterator<File> get(ScannerBase scanner, String dataset, Set<String> keys) {

    Preconditions.checkNotNull(scanner, "scanner should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(keys, "keys should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("table_name", tableName())
          .add("dataset", dataset).add("keys.size", keys.size()).formatInfo());
    }

    scanner.clearColumns();
    scanner.clearScanIterators();
    scanner.fetchColumnFamily(new Text(dataset));

    IteratorSetting setting = new IteratorSetting(21, FileStoreAnonymizingIterator.class);
    FileStoreAnonymizingIterator.setAuthorizations(setting, scanner.getAuthorizations());

    scanner.addScanIterator(setting);

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
      List<String> fileInfos =
          Splitter.on(Constants.SEPARATOR_NUL).trimResults().omitEmptyStrings().splitToList(cq);

      return new File(entry.getKey().getRow().toString(), labels, fileInfos.get(0),
          Long.parseLong(fileInfos.get(1), 10), entry.getValue().get());
    });
  }
}
