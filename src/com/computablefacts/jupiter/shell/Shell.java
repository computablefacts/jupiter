package com.computablefacts.jupiter.shell;

import static com.computablefacts.jupiter.Users.authorizations;
import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
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
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.Document;
import com.computablefacts.nona.helpers.Files;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

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

    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceName),
        "instanceName should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zooKeepers),
        "zooKeepers should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(password),
        "password should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datastore),
        "datastore should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(action),
        "action should neither be null nor empty");

    Configurations configurations =
        new Configurations(instanceName, zooKeepers, username, password);

    switch (action) {
      case "create_user":
        Preconditions.checkState(createUser(configurations, getArg(args, "au"), getArg(args, "ap")),
            "User creation failed!");
        break;
      case "blobstore_grant_alter_permission":
        Preconditions.checkState(
            grantAlterPermissionsOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on BlobStore failed!");
        break;
      case "blobstore_revoke_alter_permission":
        Preconditions.checkState(
            revokeAlterPermissionsOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on BlobStore failed!");
        break;
      case "termstore_grant_alter_permission":
        Preconditions.checkState(
            grantAlterPermissionsOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on TermStore failed!");
        break;
      case "termstore_revoke_alter_permission":
        Preconditions.checkState(
            revokeAlterPermissionsOnTermStore(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on TermStore failed!");
        break;
      case "cache_grant_alter_permission":
        Preconditions.checkState(
            grantAlterPermissionsOnCache(configurations, datastore, getArg(args, "au")),
            "Set ALTER permission on Cache failed!");
        break;
      case "cache_revoke_alter_permission":
        Preconditions.checkState(
            revokeAlterPermissionsOnCache(configurations, datastore, getArg(args, "au")),
            "Revoke ALTER permission on Cache failed!");
        break;
      case "blobstore_grant_read_permission":
        Preconditions.checkState(
            grantReadPermissionOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set READ permission on BlobStore failed!");
        break;
      case "blobstore_grant_write_permission":
        Preconditions.checkState(
            grantWritePermissionOnBlobStore(configurations, datastore, getArg(args, "au")),
            "Set WRITE permission on BlobStore failed!");
        break;
      case "termstore_grant_read_permission":
        Preconditions.checkState(
            grantReadPermissionOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set READ permission on TermStore failed!");
        break;
      case "termstore_grant_write_permission":
        Preconditions.checkState(
            grantWritePermissionOnTermStore(configurations, datastore, getArg(args, "au")),
            "Set WRITE permission on TermStore failed!");
        break;
      case "cache_grant_read_permission":
        Preconditions.checkState(
            grantReadPermissionOnCache(configurations, datastore, getArg(args, "au")),
            "Set READ permission on Cache failed!");
        break;
      case "cache_grant_write_permission":
        Preconditions.checkState(
            grantWritePermissionOnCache(configurations, datastore, getArg(args, "au")),
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
        Preconditions.checkState(remove(configurations, datastore, getArg(args, "ds")),
            "REMOVE failed!");
        break;
      case "compact":
        Preconditions.checkState(compact(configurations, datastore), "COMPACT failed!");
        break;
      case "ingest":
        Preconditions.checkState(
            ingest(configurations, datastore, getArg(args, "ds"), getArg(args, "fi")),
            "INGEST failed!");
        break;
      case "ingest_many":
        Preconditions.checkState(
            ingest(configurations, datastore,
                Sets.newHashSet(Splitter.on(',').split(getArg(args, "ds"))), getArg(args, "dir")),
            "INGEST failed!");
        break;
      case "reindex":
        Preconditions.checkState(
            reindex(configurations, datastore, getArg(args, "ds"), getArg(args, "auths")),
            "REINDEX failed!");
        break;
      case "backup":
        Preconditions.checkState(backup(configurations, datastore, getArg(args, "ds"),
            getArg(args, "fi"), getArg(args, "auths")), "BACKUP failed!");
        break;
      case "backup_many":
        Preconditions.checkState(backup(configurations, datastore,
            Sets.newHashSet(Splitter.on(',').split(getArg(args, "ds"))), getArg(args, "dir"),
            getArg(args, "auths")), "BACKUP failed!");
        break;
      default:
        throw new RuntimeException("Unknown action \"" + action + "\"");
    }
  }

  public static boolean createUser(Configurations configurations, String username,
      String password) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");

    return Users.create(configurations.connector(), username, password);
  }

  public static boolean grantAlterPermissionsOnBlobStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username,
        dataStore.blobStore().tableName(), TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnBlobStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username,
        dataStore.blobStore().tableName(), TablePermission.GRANT);
  }

  public static boolean grantAlterPermissionsOnTermStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username,
        dataStore.termStore().tableName(), TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnTermStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username,
        dataStore.termStore().tableName(), TablePermission.GRANT);
  }

  public static boolean grantAlterPermissionsOnCache(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.grantPermission(dataStore.configurations().connector(), username,
        dataStore.cache().tableName(), TablePermission.GRANT);
  }

  public static boolean revokeAlterPermissionsOnCache(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    DataStore dataStore = new DataStore(configurations, datastore);

    return Users.revokePermission(dataStore.configurations().connector(), username,
        dataStore.cache().tableName(), TablePermission.GRANT);
  }

  public static boolean grantWritePermissionOnBlobStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnBlobStore(username);
  }

  public static boolean grantReadPermissionOnBlobStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantReadPermissionOnBlobStore(username);
  }

  public static boolean grantWritePermissionOnTermStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnTermStore(username);
  }

  public static boolean grantReadPermissionOnTermStore(Configurations configurations,
      String datastore, String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantReadPermissionOnTermStore(username);
  }

  public static boolean grantWritePermissionOnCache(Configurations configurations, String datastore,
      String username) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");

    return new DataStore(configurations, datastore).grantWritePermissionOnCache(username);
  }

  public static boolean grantReadPermissionOnCache(Configurations configurations, String datastore,
      String username) {

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
      ds.blobStore().configurations().tableOperations().compact(ds.blobStore().tableName(),
          new CompactionConfig());
      ds.termStore().configurations().tableOperations().compact(ds.termStore().tableName(),
          new CompactionConfig());
      ds.cache().configurations().tableOperations().compact(ds.cache().tableName(),
          new CompactionConfig());
    } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
      return false;
    }
    return true;
  }

  public static boolean ingest(Configurations configurations, String datastore, String dataset,
      String file) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(file, "file should not be null");

    File f = new File(file);

    Preconditions.checkArgument(f.exists(), "File does not exist : %s", f.getAbsolutePath());

    Stopwatch stopwatch = Stopwatch.createStarted();
    DataStore ds = new DataStore(configurations, datastore);
    AtomicInteger count = new AtomicInteger(0);

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

      ds.blobStore().configurations().tableOperations().addSplits(ds.blobStore().tableName(),
          splits);

      ds.termStore().configurations().tableOperations().addSplits(ds.termStore().tableName(),
          splits);

      ds.cache().configurations().tableOperations().addSplits(ds.cache().tableName(), splits);

    } catch (Exception e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }

    try (Writers writers = ds.writers()) {

      ds.beginIngest();

      Files.compressedLineStream(f, StandardCharsets.UTF_8).forEach(line -> {

        String row = line.getValue();

        if (Strings.isNullOrEmpty(row)) {
          return;
        }
        try {
          Map<String, Object> json = Codecs.asObject(row);
          Document document = new Document(json);

          // if (!document.fileExists()) { // do not reindex missing files
          // if (logger_.isInfoEnabled()) {
          // logger_.info(LogFormatter.create(true).message(
          // "Number of JSON ignored : " + ignored.incrementAndGet() + " -> " + document.path())
          // .formatInfo());
          // }
          // } else {

          if (!ds.persist(writers, dataset, document.docId(), row)) {
            logger_.error(LogFormatter.create(true)
                .message("Persistence of " + document.docId() + " failed").formatError());
          }

          if (count.incrementAndGet() % 100 == 0 && logger_.isInfoEnabled()) {
            logger_.info(LogFormatter.create(true)
                .message("Number of JSON processed : " + count.get()).formatInfo());
          }
          // }
        } catch (Exception e) {
          logger_.error(LogFormatter.create(true).message(e).formatError());
        }
      });

      ds.endIngest(writers, dataset);
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true)
          .message("Total number of JSON processed : " + count.get()).formatInfo());
      logger_.info(LogFormatter.create(true)
          .message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean ingest(Configurations configurations, String datastore,
      Set<String> datasets, String directory) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(datasets, "datasets should not be null");
    Preconditions.checkNotNull(directory, "directory should not be null");

    for (String dataset : datasets) {

      String file =
          directory + File.separator + String.format("backup-%s-%s.jsonl.gz", datastore, dataset);

      Preconditions.checkState(Shell.ingest(configurations, datastore, dataset, file),
          "INGEST of dataset %s for datastore %s failed", dataset, datastore);
    }
    return true;
  }

  public static boolean reindex(Configurations configurations, String datastore, String dataset,
      String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    Stopwatch stopwatch = Stopwatch.createStarted();
    DataStore ds = new DataStore(configurations, datastore);

    if (!ds.termStore().removeDataset(dataset)) {
      logger_.error(LogFormatter.create(true)
          .message(String.format("Dataset %s cannot be removed", dataset)).formatError());
      return false;
    }

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger ignored = new AtomicInteger(0);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {
      try (Writers writers = ds.writers()) {

        ds.beginIngest();

        Iterator<Blob<Value>> iterator =
            ds.blobStore().getJsons(scanners.blob(NB_QUERY_THREADS), dataset, null, null);

        while (iterator.hasNext()) {

          Blob<Value> blob = iterator.next();

          if (count.incrementAndGet() % 100 == 0 && logger_.isInfoEnabled()) {
            if (logger_.isInfoEnabled()) {
              logger_.info(LogFormatter.create(true)
                  .message("Number of JSON written : " + count.get()).formatInfo());
            }
          }
          if (!blob.isJson()) {
            logger_.warn(LogFormatter.create(true)
                .message("Total number of JSON ignored : " + ignored.incrementAndGet())
                .formatWarn());
            continue;
          }

          Map<String, Object> json = Codecs.asObject(blob.value().toString());
          Document document = new Document(json);

          if (!ds.reindex(writers, dataset, document.docId(), json)) {
            logger_.error(LogFormatter.create(true)
                .message("Re-indexation of " + document.docId() + " failed").formatError());
          }
        }

        ds.endIngest(writers, dataset);
      }
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true)
          .message("Total number of JSON processed : " + count.get()).formatInfo());
      logger_.info(LogFormatter.create(true)
          .message("Total number of JSON ignored : " + ignored.get()).formatInfo());
      logger_.info(LogFormatter.create(true)
          .message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean backup(Configurations configurations, String datastore, String dataset,
      String file, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(file, "file should not be null");

    File f = new File(file);

    Preconditions.checkArgument(!f.exists(), "File exists : %s", f.getAbsolutePath());

    Stopwatch stopwatch = Stopwatch.createStarted();
    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {
      try (FileOutputStream fos = new FileOutputStream(f)) {
        try (BufferedWriter bw =
            new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {

          AtomicInteger count = new AtomicInteger(0);
          Iterator<Blob<Value>> iterator =
              ds.blobStore().getJsons(scanners.blob(NB_QUERY_THREADS), dataset, null, null);

          while (iterator.hasNext()) {

            Blob<Value> blob = iterator.next();

            bw.write(blob.value().toString());
            bw.newLine();

            if (count.incrementAndGet() % 100 == 0 && logger_.isInfoEnabled()) {
              if (logger_.isInfoEnabled()) {
                logger_.info(LogFormatter.create(true)
                    .message("Number of JSON written : " + count.get()).formatInfo());
              }
            }
          }

          if (logger_.isInfoEnabled()) {
            logger_.info(LogFormatter.create(true)
                .message("Number of JSON written : " + count.get()).formatInfo());
          }
        } catch (IOException e) {
          logger_.error(LogFormatter.create(true).message(e).formatError());
        }
      } catch (IOException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true)
          .message("Elapsed time : " + stopwatch.elapsed(TimeUnit.SECONDS)).formatInfo());
    }
    return true;
  }

  public static boolean backup(Configurations configurations, String datastore,
      Set<String> datasets, String directory, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(datasets, "datasets should not be null");
    Preconditions.checkNotNull(directory, "directory should not be null");

    for (String dataset : datasets) {

      String file =
          directory + File.separator + String.format("backup-%s-%s.jsonl", datastore, dataset);

      Stopwatch stopwatch = Stopwatch.createStarted();
      Preconditions.checkState(Shell.backup(configurations, datastore, dataset, file, auths),
          "BACKUP of dataset %s for datastore %s failed", dataset, datastore);
      stopwatch.stop();

      if (logger_.isInfoEnabled()) {
        logger_.info(LogFormatter.create(true)
            .message(String.format("Starting compression of dataset %s backup...", dataset))
            .formatInfo());
      }

      String fileCompressed =
          directory + File.separator + String.format("backup-%s-%s.jsonl.gz", datastore, dataset);

      stopwatch.reset();
      stopwatch.start();
      com.computablefacts.nona.helpers.Files.gzip(new File(file), new File(fileCompressed));
      com.computablefacts.nona.helpers.Files.delete(new File(file));
      stopwatch.stop();

      if (logger_.isInfoEnabled()) {
        logger_.info(LogFormatter.create(true)
            .message(String.format("Compression of dataset %s backup completed in %d ms.", dataset,
                stopwatch.elapsed(TimeUnit.MILLISECONDS)))
            .formatInfo());
      }
    }
    return true;
  }

  private static String getArg(String[] args, String name) {
    return getArg(args, name, null);
  }

  private static String getArg(String[] args, String name, String defaultValue) {
    @Var
    int argIdx = -1;
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
