package com.computablefacts.jupiter.shell;

import static com.computablefacts.jupiter.Users.authorizations;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.datastore.DataStore.blobStoreName;
import static com.computablefacts.jupiter.storage.datastore.DataStore.cacheName;
import static com.computablefacts.jupiter.storage.datastore.DataStore.termStoreName;

import com.computablefacts.asterix.Document;
import com.computablefacts.asterix.IO;
import com.computablefacts.asterix.View;
import com.computablefacts.asterix.codecs.JsonCodec;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.jupiter.storage.cache.Cache;
import com.computablefacts.jupiter.storage.datastore.AccumuloBlobProcessor;
import com.computablefacts.jupiter.storage.datastore.AccumuloHashProcessor;
import com.computablefacts.jupiter.storage.datastore.AccumuloTermProcessor;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CheckReturnValue
public class Shell {

  private static final Logger logger_ = LoggerFactory.getLogger(Shell.class);

  public static void main(String[] args) {

    String instanceName = getArg(args, "i");
    String zooKeepers = getArg(args, "z");
    String username = getArg(args, "u");
    String password = getArg(args, "p");
    String datastore = getArg(args, "d");
    String action = getArg(args, "a");

    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceName), "instanceName should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zooKeepers), "zooKeepers should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(password), "password should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datastore), "datastore should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(action), "action should neither be null nor empty");

    Configurations configurations = new Configurations(instanceName, zooKeepers, username, password);

    switch (action) {
      case "create_user":
        Preconditions.checkState(createUser(configurations, getArg(args, "au"), getArg(args, "ap")),
            "User creation failed!");
        break;
      case "blobstore_grant_alter_permission":
        Preconditions.checkState(grantAlterPermissionsOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on BlobStore failed!");
        break;
      case "blobstore_revoke_alter_permission":
        Preconditions.checkState(revokeAlterPermissionsOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on BlobStore failed!");
        break;
      case "termstore_grant_alter_permission":
        Preconditions.checkState(grantAlterPermissionsOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on TermStore failed!");
        break;
      case "termstore_revoke_alter_permission":
        Preconditions.checkState(revokeAlterPermissionsOnTermStore(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on TermStore failed!");
        break;
      case "cache_grant_alter_permission":
        Preconditions.checkState(grantAlterPermissionsOnCache(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on Cache failed!");
        break;
      case "cache_revoke_alter_permission":
        Preconditions.checkState(revokeAlterPermissionsOnCache(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on Cache failed!");
        break;
      case "blobstore_grant_read_permission":
        Preconditions.checkState(grantReadPermissionOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set READ permission on BlobStore failed!");
        break;
      case "blobstore_grant_write_permission":
        Preconditions.checkState(grantWritePermissionOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set WRITE permission on BlobStore failed!");
        break;
      case "termstore_grant_read_permission":
        Preconditions.checkState(grantReadPermissionOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set READ permission on TermStore failed!");
        break;
      case "termstore_grant_write_permission":
        Preconditions.checkState(grantWritePermissionOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set WRITE permission on TermStore failed!");
        break;
      case "cache_grant_read_permission":
        Preconditions.checkState(grantReadPermissionOnCache(configurations, datastore, getArg(args, "au")),
            "Set READ permission on Cache failed!");
        break;
      case "cache_grant_write_permission":
        Preconditions.checkState(grantWritePermissionOnCache(configurations, datastore, getArg(args, "au")),
            "Set WRITE permission on Cache failed!");
        break;
      case "create":
        Preconditions.checkState(create(configurations, datastore), "CREATE failed!");
        break;
      case "destroy":
        Preconditions.checkState(destroy(configurations, datastore), "DESTROY failed!");
        break;
      case "truncate":
        Preconditions.checkState(truncate(configurations, datastore), "TRUNCATE failed!");
        break;
      case "remove":
        Preconditions.checkState(remove(configurations, datastore, getArg(args, "ds")), "REMOVE failed!");
        break;
      case "compact":
        Preconditions.checkState(compact(configurations, datastore), "COMPACT failed!");
        break;
      case "ingest":
        Preconditions.checkState(ingest(configurations, datastore, getArg(args, "ds"), getArg(args, "fi"), false,
            Boolean.parseBoolean(getArg(args, "hash", "false"))), "INGEST failed!");
        break;
      case "ingest_many":
        Preconditions.checkState(
            ingest(configurations, datastore, Sets.newHashSet(Splitter.on(',').split(getArg(args, "ds"))),
                getArg(args, "dir"), Boolean.parseBoolean(getArg(args, "hash", "false"))), "INGEST failed!");
        break;
      case "reindex":
        Preconditions.checkState(reindex(configurations, datastore, getArg(args, "ds"), getArg(args, "auths")),
            "REINDEX failed!");
        break;
      case "backup":
        Preconditions.checkState(
            backup(configurations, datastore, getArg(args, "ds"), getArg(args, "fi"), getArg(args, "auths")),
            "BACKUP failed!");
        break;
      case "backup_many":
        Preconditions.checkState(
            backup(configurations, datastore, Sets.newHashSet(Splitter.on(',').split(getArg(args, "ds"))),
                getArg(args, "dir"), getArg(args, "auths")), "BACKUP failed!");
        break;
      default:
        throw new RuntimeException("Unknown action \"" + action + "\"");
    }
  }

  public static boolean createUser(Configurations configurations, String username, String password) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");

    return Users.create(configurations.connector(), username, password);
  }

  public static boolean grantAlterPermissionsOnBlobStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username, dataStore.blobStore().tableName(),
        TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnBlobStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username, dataStore.blobStore().tableName(),
        TablePermission.GRANT);
  }

  public static boolean grantAlterPermissionsOnTermStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username, dataStore.termStore().tableName(),
        TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnTermStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username, dataStore.termStore().tableName(),
        TablePermission.GRANT);
  }

  public static boolean grantAlterPermissionsOnCache(Configurations configurations, String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username, dataStore.cache().tableName(),
        TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnCache(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username, dataStore.cache().tableName(),
        TablePermission.GRANT);
  }

  public static boolean grantWritePermissionOnBlobStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnBlobStore(username);
  }

  public static boolean grantReadPermissionOnBlobStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantReadPermissionOnBlobStore(username);
  }

  public static boolean grantWritePermissionOnTermStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnTermStore(username);
  }

  public static boolean grantReadPermissionOnTermStore(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantReadPermissionOnTermStore(username);
  }

  public static boolean grantWritePermissionOnCache(Configurations configurations, String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnCache(username);
  }

  public static boolean grantReadPermissionOnCache(Configurations configurations, String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantReadPermissionOnCache(username);
  }

  public static boolean create(Configurations configurations, String datastore) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).create();
  }

  public static boolean destroy(Configurations configurations, String datastore) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).destroy();
  }

  public static boolean truncate(Configurations configurations, String datastore) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).truncate();
  }

  public static boolean remove(Configurations configurations, String datastore, String dataset) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return new DataStore(configurations, datastore).remove(dataset);
  }

  public static boolean compact(Configurations configurations, String datastore) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    try {
      DataStore ds = new DataStore(configurations, datastore);
      ds.blobStore().configurations().tableOperations().compact(ds.blobStore().tableName(), new CompactionConfig());
      ds.termStore().configurations().tableOperations().compact(ds.termStore().tableName(), new CompactionConfig());
      ds.cache().configurations().tableOperations().compact(ds.cache().tableName(), new CompactionConfig());
    } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
      return false;
    }
    return true;
  }

  public static boolean ingest(Configurations configurations, String datastore, String dataset, String file,
      boolean split, boolean hash) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(file, "file should not be null");

    File f = new File(file);

    Preconditions.checkArgument(f.exists(), "File does not exist : %s", f.getAbsolutePath());

    AtomicInteger count = new AtomicInteger(0);
    Stopwatch stopwatch = Stopwatch.createStarted();
    BlobStore blobStore = new BlobStore(configurations, blobStoreName(datastore));
    TermStore termStore = new TermStore(configurations, termStoreName(datastore));
    Cache cache = new Cache(configurations, cacheName(datastore));
    AccumuloBlobProcessor blobProcessor = new AccumuloBlobProcessor(blobStore);
    AccumuloTermProcessor termProcessor = new AccumuloTermProcessor(termStore);
    AccumuloHashProcessor hashProcessor = hash ? new AccumuloHashProcessor(termStore) : null;

    try (DataStore ds = new DataStore(datastore, blobStore, termStore, cache, blobProcessor, termProcessor,
        hashProcessor)) {
      if (split) {
        try {

          SortedSet<Text> splits = new TreeSet<>();

          for (char i = '0'; i < '9' + 1; i++) {
            splits.add(new Text(dataset + SEPARATOR_NUL + i));
          }

          for (char i = 'a'; i < 'z' + 1; i++) {
            splits.add(new Text(dataset + SEPARATOR_NUL + i));
          }

          for (char i = 'A'; i < 'Z' + 1; i++) {
            splits.add(new Text(dataset + SEPARATOR_NUL + i));
          }

          ds.blobStore().configurations().tableOperations().addSplits(ds.blobStore().tableName(), splits);

          ds.termStore().configurations().tableOperations().addSplits(ds.termStore().tableName(), splits);

          ds.cache().configurations().tableOperations().addSplits(ds.cache().tableName(), splits);

        } catch (Exception e) {
          logger_.error(LogFormatter.create(true).message(e).formatError());
        }
      }

      View.of(f, true).index().forEachRemaining((line, breaker) -> {

        String row = line.getValue();

        if (Strings.isNullOrEmpty(row)) {
          return;
        }
        try {
          Map<String, Object> json = JsonCodec.asObject(row);
          Document document = new Document(json);

          // if (!document.fileExists()) { // do not reindex missing files
          // if (logger_.isInfoEnabled()) {
          // logger_.info(LogFormatter.create(true).message(
          // "Number of JSON ignored : " + ignored.incrementAndGet() + " -> " +
          // document.path())
          // .formatInfo());
          // }
          // } else {

          if (!ds.persist(dataset, document.docId(), row)) {
            logger_.error(
                LogFormatter.create(true).message("Persistence of " + document.docId() + " failed").formatError());
            breaker.stop();
          }

          if ((count.incrementAndGet() % 100 == 0 || breaker.shouldBreak()) && logger_.isInfoEnabled()) {
            logger_.info(LogFormatter.create(true).message("Number of JSON processed : " + count.get()).formatInfo());
          }
          // }
        } catch (Exception e) {
          logger_.error(LogFormatter.create(true).message(e).formatError());
        }
      });
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).message("Total number of JSON processed : " + count.get()).formatInfo());
      logger_.info(
          LogFormatter.create(true).message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean ingest(Configurations configurations, String datastore, Set<String> datasets, String directory,
      boolean hash) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(datasets, "datasets should not be null");
    Preconditions.checkNotNull(directory, "directory should not be null");

    @Var boolean split = true;

    for (String dataset : datasets) {

      String file = directory + File.separator + String.format("backup-%s-%s.jsonl.gz", datastore, dataset);

      Preconditions.checkState(Shell.ingest(configurations, datastore, dataset, file, split, hash),
          "INGEST of dataset %s for datastore %s failed", dataset, datastore);

      split = false;
    }
    return true;
  }

  public static boolean reindex(Configurations configurations, String datastore, String dataset, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger ignored = new AtomicInteger(0);
    Authorizations authorizations = authorizations(auths);
    Stopwatch stopwatch = Stopwatch.createStarted();

    try (DataStore ds = new DataStore(configurations, datastore)) {

      if (!ds.termStore().removeDataset(dataset)) {
        logger_.error(
            LogFormatter.create(true).message(String.format("Dataset %s cannot be removed", dataset)).formatError());
        return false;
      }

      View<Blob<Value>> iterator = ds.jsons(authorizations, dataset, null);

      while (iterator.hasNext()) {

        Blob<Value> blob = iterator.next();

        if (count.incrementAndGet() % 100 == 0 && logger_.isInfoEnabled()) {
          if (logger_.isInfoEnabled()) {
            logger_.info(LogFormatter.create(true).message("Number of JSON written : " + count.get()).formatInfo());
          }
        }
        if (!blob.isJson()) {
          logger_.warn(LogFormatter.create(true).message("Total number of JSON ignored : " + ignored.incrementAndGet())
              .formatWarn());
          continue;
        }

        Map<String, Object> json = JsonCodec.asObject(blob.value().toString());
        Document document = new Document(json);

        if (!ds.reindex(dataset, document.docId(), json)) {
          logger_.error(
              LogFormatter.create(true).message("Re-indexation of " + document.docId() + " failed").formatError());
          break;
        }
      }
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).message("Total number of JSON processed : " + count.get()).formatInfo());
      logger_.info(LogFormatter.create(true).message("Total number of JSON ignored : " + ignored.get()).formatInfo());
      logger_.info(
          LogFormatter.create(true).message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean backup(Configurations configurations, String datastore, String dataset, String file,
      String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(file, "file should not be null");

    File f = new File(file);

    Preconditions.checkArgument(!f.exists(), "File exists : %s", f.getAbsolutePath());

    Authorizations authorizations = authorizations(auths);
    Stopwatch stopwatch = Stopwatch.createStarted();

    try (DataStore ds = new DataStore(configurations, datastore)) {
      try (BufferedWriter bw = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8))) {

        ds.jsons(authorizations, dataset, null).index().forEachRemaining(e -> {

          int count = e.getKey();
          Blob<Value> blob = e.getValue();

          try {
            bw.write(blob.value().toString());
            bw.newLine();
          } catch (IOException ex) {
            logger_.error(LogFormatter.create(true).message(ex).formatError());
          }

          if (count % 100 == 0 && logger_.isInfoEnabled()) {
            if (logger_.isInfoEnabled()) {
              logger_.info(LogFormatter.create(true).message("Number of JSON written : " + count).formatInfo());
            }
          }
        });
      } catch (IOException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(
          LogFormatter.create(true).message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean backup(Configurations configurations, String datastore, Set<String> datasets, String directory,
      String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(datasets, "datasets should not be null");
    Preconditions.checkNotNull(directory, "directory should not be null");

    for (String dataset : datasets) {

      String file = directory + File.separator + String.format("backup-%s-%s.jsonl", datastore, dataset);

      Stopwatch stopwatch = Stopwatch.createStarted();
      Preconditions.checkState(Shell.backup(configurations, datastore, dataset, file, auths),
          "BACKUP of dataset %s for datastore %s failed", dataset, datastore);
      stopwatch.stop();

      if (logger_.isInfoEnabled()) {
        logger_.info(
            LogFormatter.create(true).message(String.format("Starting compression of dataset %s backup...", dataset))
                .formatInfo());
      }

      String fileCompressed = directory + File.separator + String.format("backup-%s-%s.jsonl.gz", datastore, dataset);

      stopwatch.reset();
      stopwatch.start();
      if (IO.gzip(new File(file), new File(fileCompressed))) {
        new File(file).delete();
      }
      stopwatch.stop();

      if (logger_.isInfoEnabled()) {
        logger_.info(LogFormatter.create(true).message(
            String.format("Compression of dataset %s backup completed in %d ms.", dataset,
                stopwatch.elapsed(TimeUnit.MILLISECONDS))).formatInfo());
      }
    }
    return true;
  }

  private static String getArg(String[] args, String name) {
    return getArg(args, name, null);
  }

  private static String getArg(String[] args, String name, String defaultValue) {
    @Var int argIdx = -1;
    for (int idx = 0; idx < args.length; idx++) {
      if (("-" + name).equals(args[idx])) {
        argIdx = idx;
        break;
      }
    }
    if (argIdx == -1) {
      return defaultValue;
    }
    if (argIdx < args.length - 1) {
      return args[argIdx + 1].trim();
    }
    throw new RuntimeException("Missing argument value. Argument name: " + name);
  }
}
