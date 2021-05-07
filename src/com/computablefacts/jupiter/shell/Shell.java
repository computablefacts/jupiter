package com.computablefacts.jupiter.shell;

import static com.computablefacts.jupiter.Users.authorizations;
import static com.computablefacts.jupiter.storage.Constants.AUTH_ADM;
import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.logfmt.LogFormatter;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.Document;
import com.computablefacts.nona.helpers.Files;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
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
      case "add_locality_group":
        Preconditions.checkState(addLocalityGroup(configurations, datastore, getArg(args, "ds")),
            "LOCALITY GROUP creation failed!");
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
      case "ingest":
        Preconditions.checkState(
            ingest(configurations, datastore, getArg(args, "ds"), getArg(args, "fi")),
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
      default:
        throw new RuntimeException("Unknown action \"" + action + "\"");
    }
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

  public static boolean addLocalityGroup(Configurations configurations, String datastore,
      String dataset) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    return new DataStore(configurations, datastore).addLocalityGroup(dataset);
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
    AtomicInteger ignored = new AtomicInteger(0);

    try (Writers writers = ds.writers()) {

      ds.beginIngest();

      Files.compressedLineStream(f, StandardCharsets.UTF_8).forEach(line -> {

        String row = line.getValue();

        if (Strings.isNullOrEmpty(row)) {
          return;
        }

        Map<String, Object> json = Codecs.asObject(row);
        Document document = new Document(json);

        if (!document.fileExists()) { // do not reindex missing files
          if (logger_.isInfoEnabled()) {
            logger_.info(LogFormatter.create(true).message(
                "Number of JSON ignored : " + ignored.incrementAndGet() + " -> " + document.path())
                .formatInfo());
          }
        } else {

          if (!ds.persist(writers, dataset, document.docId(), row)) {
            logger_.error(LogFormatter.create(true)
                .message("Persistence of " + document.docId() + " failed").formatError());
          }

          if (count.incrementAndGet() % 100 == 0 && logger_.isInfoEnabled()) {
            if (logger_.isInfoEnabled()) {
              logger_.info(LogFormatter.create(true)
                  .message("Number of JSON processed : " + count.get()).formatInfo());
            }
          }
        }
      });

      ds.endIngest(writers, dataset);
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

  public static boolean reindex(Configurations configurations, String datastore, String dataset,
      String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    Stopwatch stopwatch = Stopwatch.createStarted();
    DataStore ds = new DataStore(configurations, datastore);

    try (BatchDeleter deleter = ds.termStore().deleter(AUTH_ADM)) {
      if (!ds.termStore().removeDataset(deleter, dataset)) {
        logger_.error(LogFormatter.create(true)
            .message(String.format("Dataset %s cannot be removed", dataset)).formatError());
        return false;
      }
    }

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger ignored = new AtomicInteger(0);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {
      try (Writers writers = ds.writers()) {

        ds.beginIngest();

        Iterator<Blob<Value>> iterator =
            ds.blobStore().get(scanners.blob(NB_QUERY_THREADS), dataset);

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

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {
      try (FileOutputStream fos = new FileOutputStream(f)) {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos))) {

          AtomicInteger count = new AtomicInteger(0);
          Iterator<Blob<Value>> iterator =
              ds.blobStore().get(scanners.blob(NB_QUERY_THREADS), dataset);

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
