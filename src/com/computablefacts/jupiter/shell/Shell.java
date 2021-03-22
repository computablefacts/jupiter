package com.computablefacts.jupiter.shell;

import static com.computablefacts.jupiter.Users.authorizations;
import static com.computablefacts.nona.helpers.Codecs.defaultTokenizer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.jupiter.storage.termstore.FieldCard;
import com.computablefacts.jupiter.storage.termstore.FieldCount;
import com.computablefacts.jupiter.storage.termstore.FieldLabels;
import com.computablefacts.jupiter.storage.termstore.IngestStats;
import com.computablefacts.jupiter.storage.termstore.TermCard;
import com.computablefacts.jupiter.storage.termstore.TermCount;
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
      case "backup":
        Preconditions.checkState(backup(configurations, datastore, getArg(args, "ds"),
            getArg(args, "fi"), getArg(args, "auths")), "BACKUP failed!");
        break;
      case "field_count":
        Preconditions.checkState(fieldCount(configurations, datastore, getArg(args, "ds"),
            getArg(args, "field"), getArg(args, "auths")));
        break;
      case "field_cardinality":
        Preconditions.checkState(fieldCardinality(configurations, datastore, getArg(args, "ds"),
            getArg(args, "field"), getArg(args, "auths")));
        break;
      case "field_labels":
        Preconditions.checkState(fieldLabels(configurations, datastore, getArg(args, "ds"),
            getArg(args, "field"), getArg(args, "auths")));
        break;
      case "term_count":
        Preconditions.checkState(termCount(configurations, datastore, getArg(args, "ds"),
            getArg(args, "term"), getArg(args, "auths")));
        break;
      case "term_cardinality":
        Preconditions.checkState(termCardinality(configurations, datastore, getArg(args, "ds"),
            getArg(args, "term"), getArg(args, "auths")));
        break;
      case "search":
        Preconditions.checkState(search(configurations, datastore, getArg(args, "ds"),
            getArg(args, "query"), getArg(args, "auths")));
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
      try (IngestStats stats = ds.newIngestStats()) {
        Files.compressedLineStream(f, StandardCharsets.UTF_8).forEach(line -> {

          String row = line.getValue();

          if (Strings.isNullOrEmpty(row)) {
            return;
          }

          Map<String, Object> json = Codecs.asObject(row);
          Document document = new Document(json);

          if (!document.fileExists()) { // do not reindex missing files
            if (logger_.isInfoEnabled()) {
              logger_.info(LogFormatterManager.logFormatter().message("Number of JSON ignored : "
                  + ignored.incrementAndGet() + " -> " + document.path()).formatInfo());
            }
          } else {

            if (!ds.persist(writers, stats, dataset, document.docId(), row, key -> true,
                Codecs.defaultTokenizer)) {
              logger_.error(LogFormatterManager.logFormatter()
                  .message("Persistence of " + document.docId() + " failed").formatError());
            }

            if (count.incrementAndGet() % 100 == 0) {

              stats.flush();

              if (logger_.isInfoEnabled()) {
                logger_.info(LogFormatterManager.logFormatter()
                    .message("Number of JSON processed : " + count.get()).formatInfo());
              }
            }
          }
        });
      }
    }

    stopwatch.stop();

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter()
          .message("Total number of JSON processed : " + count.get()).formatInfo());
      logger_.info(LogFormatterManager.logFormatter()
          .message("Total number of JSON ignored : " + ignored.get()).formatInfo());
      logger_.info(LogFormatterManager.logFormatter()
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

    try (Scanners scanners = ds.batchScanners(authorizations(auths))) {

      AtomicInteger count = new AtomicInteger(0);
      List<String> jsons = new ArrayList<>();
      Iterator<Blob<Value>> iterator = ds.blobScan(scanners, dataset);

      while (iterator.hasNext()) {

        int cnt = count.incrementAndGet();
        Blob<Value> blob = iterator.next();
        jsons.add(blob.value().toString());

        if (jsons.size() >= 1000) {

          if (cnt == jsons.size()) {
            com.computablefacts.nona.helpers.Files.create(f, jsons);
          } else {
            com.computablefacts.nona.helpers.Files.append(f, jsons);
          }

          jsons.clear();

          if (logger_.isInfoEnabled()) {
            logger_.info(LogFormatterManager.logFormatter()
                .message("Number of JSON written : " + cnt).formatInfo());
          }
        }
      }

      if (!jsons.isEmpty()) {

        if (count.get() == jsons.size()) {
          com.computablefacts.nona.helpers.Files.create(f, jsons);
        } else {
          com.computablefacts.nona.helpers.Files.append(f, jsons);
        }

        jsons.clear();

        if (logger_.isInfoEnabled()) {
          logger_.info(LogFormatterManager.logFormatter()
              .message("Number of JSON written : " + count.get()).formatInfo());
        }
      }
    }
    return true;
  }

  public static boolean fieldCount(Configurations configurations, String datastore, String dataset,
      String field, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(field),
        "field should neither be null nor empty");
    Preconditions.checkArgument(field.length() >= 3, "Field length must be >= 3 : %s", field);

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {

      Iterator<FieldCount> iterator = ds.fieldCount(scanners, dataset, field);

      while (iterator.hasNext()) {
        FieldCount fieldCount = iterator.next();
        System.out.println(fieldCount.field() + " -> " + fieldCount.count());
      }
    }
    return true;
  }

  public static boolean fieldCardinality(Configurations configurations, String datastore,
      String dataset, String field, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(field),
        "field should neither be null nor empty");
    Preconditions.checkArgument(field.length() >= 3, "Field length must be >= 3 : %s", field);

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {

      Iterator<FieldCard> iterator = ds.fieldCard(scanners, dataset, field);

      while (iterator.hasNext()) {
        FieldCard fieldCard = iterator.next();
        System.out.println(fieldCard.field() + " -> " + fieldCard.cardinality());
      }
    }
    return true;
  }

  public static boolean fieldLabels(Configurations configurations, String datastore, String dataset,
      String field, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(field),
        "field should neither be null nor empty");
    Preconditions.checkArgument(field.length() >= 3, "Field length must be >= 3 : %s", field);

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {

      Iterator<FieldLabels> iterator = ds.fieldLabels(scanners, dataset, field);

      while (iterator.hasNext()) {
        FieldLabels fieldLabels = iterator.next();
        System.out.println(fieldLabels.field() + " -> " + fieldLabels.termLabels());
      }
    }
    return true;
  }

  public static boolean termCount(Configurations configurations, String datastore, String dataset,
      String term, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(term),
        "term should neither be null nor empty");
    Preconditions.checkArgument(term.length() >= 3, "Field length must be >= 3 : %s", term);

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {

      Iterator<Pair<String, List<TermCount>>> iterator = ds.termCount(scanners, dataset, term);

      while (iterator.hasNext()) {

        Pair<String, List<TermCount>> termCounts = iterator.next();

        for (TermCount tc : termCounts.getSecond()) {
          System.out.println(tc.term() + " -> " + tc.count() + " (" + tc.field() + ")");
        }
      }
    }
    return true;
  }

  public static boolean termCardinality(Configurations configurations, String datastore,
      String dataset, String term, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(term),
        "term should neither be null nor empty");
    Preconditions.checkArgument(term.length() >= 3, "Field length must be >= 3 : %s", term);

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {

      Iterator<Pair<String, List<TermCard>>> iterator = ds.termCard(scanners, dataset, term);

      while (iterator.hasNext()) {

        Pair<String, List<TermCard>> termCards = iterator.next();

        for (TermCard tc : termCards.getSecond()) {
          System.out.println(tc.term() + " -> " + tc.cardinality() + " (" + tc.field() + ")");
        }
      }
    }
    return true;
  }

  public static boolean search(Configurations configurations, String datastore, String dataset,
      String query, String auths) {

    Preconditions.checkNotNull(configurations, "configurations should not be null");
    Preconditions.checkNotNull(datastore, "datastore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(query),
        "query should neither be null nor empty");

    DataStore ds = new DataStore(configurations, datastore);

    try (Scanners scanners = ds.scanners(authorizations(auths))) {
      try (Scanners batchScanners = ds.batchScanners(authorizations(auths))) {
        try (Writers writers = ds.writers()) {

          AbstractNode tree = QueryBuilder.build(query);
          System.out.println(
              "Max. results : " + tree.cardinality(ds, scanners, dataset, defaultTokenizer));

          Iterator<String> iterator =
              tree.execute(ds, scanners, writers, dataset, null, defaultTokenizer);

          while (iterator.hasNext()) {

            Set<String> docIds = new HashSet<>(1000);

            while (iterator.hasNext() && docIds.size() < 1000) {
              docIds.add(iterator.next());
            }

            Iterator<Blob<Value>> iter = ds.blobScan(batchScanners, dataset, docIds);

            while (iter.hasNext()) {
              Blob<Value> blob = iter.next();
              String json = blob.value().toString();
              System.out.println(
                  blob.key() + " -> " + json.substring(0, Math.min(160, json.length())) + "...");
            }
          }
        }
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
