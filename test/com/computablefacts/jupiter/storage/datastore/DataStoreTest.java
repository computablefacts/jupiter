package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.AUTH_ADM;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;

import com.computablefacts.asterix.codecs.StringCodec;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Data;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

public class DataStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testCreateIsReadyAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    DataStore dataStore = new DataStore(configurations, tableName);

    Assert.assertFalse(dataStore.isReady());
    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());
    Assert.assertTrue(dataStore.destroy());
    Assert.assertFalse(dataStore.isReady());
  }

  @Test
  public void testTruncate() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();
    AbstractNode query = QueryBuilder.build("doe");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    Assert.assertTrue(dataStore.truncate());

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertTrue(docsIds.isEmpty()); // because the cache has been trashed

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertTrue(docsIds.isEmpty()); // because the cache has been trashed
  }

  @Test
  public void testRemoveDataset() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();
    AbstractNode query = QueryBuilder.build("doe");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    Assert.assertTrue(dataStore.remove("dataset_1"));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(0, docsIds.size());

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
  }

  @Test
  public void testAndQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh? AND doe");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertTrue(docsIds.isEmpty());
  }

  @Test
  public void testAndNotQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("doe AND NOT jan?");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertTrue(docsIds.isEmpty());
  }

  @Test
  public void testOrQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_2").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
  }

  @Test
  public void testRowLevelAuthorizations() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(new Authorizations("ADM", "DATASET_1_ROW_1", "DATASET_1_ROW_2"), username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    @Var List<String> docsIds = query.execute(dataStore, new Authorizations("DATASET_1_ROW_1"), "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));

    docsIds = query.execute(dataStore, new Authorizations("DATASET_1_ROW_2"), "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_2", docsIds.get(0));
  }

  @Test
  public void testCount() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var AbstractNode query = QueryBuilder.build("joh* OR jan*");

    Assert.assertEquals(2, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("*ohn OR *ane");

    Assert.assertEquals(2, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("joh* AND jan*");

    Assert.assertEquals(1, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("*ohn AND *ane");

    Assert.assertEquals(1, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("joh* AND NOT jan*");

    Assert.assertEquals(1, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("*ohn AND NOT *ane");

    Assert.assertEquals(1, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));
  }

  @Test
  public void testRangeCount() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    Assert.assertEquals(2, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("age:[15 TO 20]");

    Assert.assertEquals(2, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    query = QueryBuilder.build("age:[15 TO *]");

    Assert.assertEquals(2, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    Assert.assertEquals(1, query.cardinality(dataStore, AUTH_ADM, "dataset_1"));
  }

  @Test
  public void testRangeQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    @Var List<String> docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
    Assert.assertEquals("row_2", docsIds.get(1));

    query = QueryBuilder.build("age:[15 TO 20]");

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
    Assert.assertEquals("row_2", docsIds.get(1));

    query = QueryBuilder.build("age:[15 TO *]");

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
    Assert.assertEquals("row_2", docsIds.get(1));

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    docsIds = query.execute(dataStore, AUTH_ADM, "dataset_1").toList();

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals("row_1", docsIds.get(0));
  }

  @Test
  public void testMatchValue() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var List<String> docsIds = dataStore.matchValue(AUTH_ADM, "dataset_1", "Actors[*]¤name", "Tom Cruise").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValue(AUTH_ADM, "dataset_1", "Actors[*]¤weight", 67.5).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValue(AUTH_ADM, "dataset_1", "Actors[*]¤age", 73).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValue(AUTH_ADM, "dataset_1", "Actors[*]¤hasGreyHair", false).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValue(AUTH_ADM, "dataset_1", "Actors[*]¤hasChildren", true).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testMatchHash() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var List<String> docsIds = dataStore.matchHash(AUTH_ADM, "dataset_1", "Actors[*]¤name",
        "8f8a04ea49585975fcf1e452b988e085").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHash(AUTH_ADM, "dataset_1", "Actors[*]¤weight", "4103e8509cbdf6b3372222061bbe1da6")
        .toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHash(AUTH_ADM, "dataset_1", "Actors[*]¤age", "3974c437d717863985a0b5618f289b46").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHash(AUTH_ADM, "dataset_1", "Actors[*]¤hasGreyHair", "e495b7e5056dbfc4e854950696d4c3cc")
        .toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHash(AUTH_ADM, "dataset_1", "Actors[*]¤hasChildren", "5db32d6ecc1f5ef816ebe6268a3343c2")
        .toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testMatchValueSortedByDocsIds() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var List<String> docsIds = dataStore.matchValueSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤name", "Tom Cruise")
        .toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValueSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤weight", 67.5).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValueSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤age", 73).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValueSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤hasGreyHair", false).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchValueSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤hasChildren", true).toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testMatchHashSortedByDocsIds() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var List<String> docsIds = dataStore.matchHashSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤name",
        "8f8a04ea49585975fcf1e452b988e085").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHashSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤weight",
        "4103e8509cbdf6b3372222061bbe1da6").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHashSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤age",
        "3974c437d717863985a0b5618f289b46").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHashSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤hasGreyHair",
        "e495b7e5056dbfc4e854950696d4c3cc").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds = dataStore.matchHashSortedByDocId(AUTH_ADM, "dataset_1", "Actors[*]¤hasChildren",
        "5db32d6ecc1f5ef816ebe6268a3343c2").toList();

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testReindex() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));

    dataStore.flush();

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      List<String> items = blobs.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset_1\0row_1|JSO||{\"birthdate\":\"2004-04-01T00:00:00Z\",\"last_name\":\"doe\",\"id\":\"1\",\"first_name\":\"john\",\"age\":17}"));
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(19, terms.size());

      List<String> items = terms.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Check all terms but the ones in the LU, DT and TT column families
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FCNT|birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FIDX|row_1\000birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FCNT|id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FIDX|row_1\000id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FCNT|age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FIDX|row_1\000age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FIDX|row_1\000first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BIDX|row_1\000first_name\0001|1"));

      // Check hashes
      Assert.assertTrue(items.contains("dataset_1\0004b5c86196dd52c0cf2673d2d0a569431|H|row_1\000last_name|"));
      Assert.assertTrue(items.contains("dataset_1\0006174693c483abae057d822c6cc4c67b9|H|row_1\000age|"));
      Assert.assertTrue(items.contains("dataset_1\000717c7b8afebbfb7137f6f0f99beb2a94|H|row_1\000id|"));
      Assert.assertTrue(items.contains("dataset_1\00088fecf016203005fdbeb018c1376c333|H|row_1\000first_name|"));
      Assert.assertTrue(items.contains("dataset_1\0008c979aa1006083b505eadf7fdbbd786c|H|row_1\000birthdate|"));
    }

    // Remove all terms
    Assert.assertTrue(dataStore.termStore().removeDataset("dataset_1"));

    // Reindex -> do not update blobs but rebuild the whole terms index
    Assert.assertTrue(dataStore.reindex("dataset_1", "row_1", Data.json2(1)));

    dataStore.flush();

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      List<String> items = blobs.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset_1\0row_1|JSO||{\"birthdate\":\"2004-04-01T00:00:00Z\",\"last_name\":\"doe\",\"id\":\"1\",\"first_name\":\"john\",\"age\":17}"));
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(19, terms.size());

      List<String> items = terms.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Check all items but the ones in the LU, DT and TT column families
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FCNT|birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FIDX|row_1\000birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FCNT|id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FIDX|row_1\000id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FCNT|age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FIDX|row_1\000age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FIDX|row_1\000first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BIDX|row_1\000first_name\0001|1"));

      // Check hashes
      Assert.assertTrue(items.contains("dataset_1\0004b5c86196dd52c0cf2673d2d0a569431|H|row_1\000last_name|"));
      Assert.assertTrue(items.contains("dataset_1\0006174693c483abae057d822c6cc4c67b9|H|row_1\000age|"));
      Assert.assertTrue(items.contains("dataset_1\000717c7b8afebbfb7137f6f0f99beb2a94|H|row_1\000id|"));
      Assert.assertTrue(items.contains("dataset_1\00088fecf016203005fdbeb018c1376c333|H|row_1\000first_name|"));
      Assert.assertTrue(items.contains("dataset_1\0008c979aa1006083b505eadf7fdbbd786c|H|row_1\000birthdate|"));
    }
  }

  @Test
  public void testIndexDecimalNumbers() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    Assert.assertTrue(dataStore.persist("dataset", "row_1", Data.json6()));

    dataStore.flush();

    // Check BlobStore
    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      List<String> items = blobs.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset\u0000row_1|JSO||{\"prénom\":\"john\",\"score_2\":\".99\",\"score_1\":\"0.98\",\"id\":\"1\",\"score_3\":\"97.\",\"nom\":\"doe\"}"));
    }

    // Check TermStore
    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(26, terms.size());

      List<String> items = terms.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Check id
      Assert.assertTrue(items.contains("dataset\000?1*|FCNT|id\0002|1"));
      Assert.assertTrue(items.contains("dataset\000?1*|FIDX|row_1\000id\0002|1"));

      // Check scores
      Assert.assertTrue(items.contains("dataset\00079|BCNT|score_3\0001|1"));
      Assert.assertTrue(items.contains("dataset\00079|BIDX|row_1\000score_3\0001|1"));
      Assert.assertTrue(items.contains("dataset\00097|FCNT|score_3\0001|1"));
      Assert.assertTrue(items.contains("dataset\00097|FIDX|row_1\000score_3\0001|1"));

      Assert.assertTrue(items.contains("dataset\00099|BCNT|score_2\0001|1"));
      Assert.assertTrue(items.contains("dataset\00099|BIDX|row_1\000score_2\0001|1"));
      Assert.assertTrue(items.contains("dataset\00099|FCNT|score_2\0001|1"));
      Assert.assertTrue(items.contains("dataset\00099|FIDX|row_1\000score_2\0001|1"));

      // Check hashes
      Assert.assertTrue(items.contains("dataset\0004b5c86196dd52c0cf2673d2d0a569431|H|row_1\000nom|"));
      Assert.assertTrue(items.contains("dataset\000705b98fa2fcabec353bc2af216a19c6c|H|row_1\000score_3|"));
      Assert.assertTrue(items.contains("dataset\000717c7b8afebbfb7137f6f0f99beb2a94|H|row_1\000id|"));
      Assert.assertTrue(items.contains("dataset\00088fecf016203005fdbeb018c1376c333|H|row_1\000prénom|"));
      Assert.assertTrue(items.contains("dataset\00099dc3020f1177845ed45f8b7651e0721|H|row_1\000score_2|"));
      Assert.assertTrue(items.contains("dataset\0009cd4452654e1932a5ba9bb7513af30e2|H|row_1\000score_1|"));
    }
  }

  @Test
  public void testIndexNumbersPrefixedWithZeroes() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    Assert.assertTrue(dataStore.persist("dataset", "row_1", Data.json4()));

    dataStore.flush();

    // Check BlobStore
    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      List<String> items = blobs.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset\0row_1|JSO||{\"code_postal\":\"01800\",\"ville\":\"Villieu\",\"rue\":95,\"prénom\":\"john\",\"nb_connexions\":\"0\",\"adresse\":\"avenue Charles-de-Gaulle\",\"sexe\":\"F\",\"id\":\"0001\",\"nom\":\"doe\",\"age\":27}"));
    }

    // Check TermStore
    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(56, terms.size());

      List<String> items = terms.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Check id
      Assert.assertTrue(items.contains("dataset\0000001|FCNT|id\0001|1"));
      Assert.assertTrue(items.contains("dataset\0000001|FIDX|row_1\000id\0001|1"));
      Assert.assertTrue(items.contains("dataset\0001000|BCNT|id\0001|1"));
      Assert.assertTrue(items.contains("dataset\0001000|BIDX|row_1\000id\0001|1"));

      // Check postcodes
      Assert.assertTrue(items.contains("dataset\00000810|BCNT|code_postal\0001|1"));
      Assert.assertTrue(items.contains("dataset\00000810|BIDX|row_1\000code_postal\0001|1"));
      Assert.assertTrue(items.contains("dataset\00001800|FCNT|code_postal\0001|1"));
      Assert.assertTrue(items.contains("dataset\00001800|FIDX|row_1\000code_postal\0001|1"));

      // Check # connections
      Assert.assertTrue(items.contains("dataset\000?0*|FCNT|nb_connexions\0002|1"));
      Assert.assertTrue(items.contains("dataset\000?0*|FIDX|row_1\000nb_connexions\0002|1"));

      // Check hashes
      Assert.assertTrue(items.contains("dataset\0003411263c2a61d88640486814343c1dcd|H|row_1\000adresse|"));
      Assert.assertTrue(items.contains("dataset\000365ccfea623eaebf17f36c5a0cdc4ddc|H|row_1\000id|"));
      Assert.assertTrue(items.contains("dataset\0004b5c86196dd52c0cf2673d2d0a569431|H|row_1\000nom|"));
      Assert.assertTrue(items.contains("dataset\00080a346d5bedec92a095e873ce5e98d3a|H|row_1\000nb_connexions|"));
      Assert.assertTrue(items.contains("dataset\00088fecf016203005fdbeb018c1376c333|H|row_1\000prénom|"));
      Assert.assertTrue(items.contains("dataset\000a408d3633275bd7d9d07fe871f1a4f35|H|row_1\000age|"));
      Assert.assertTrue(items.contains("dataset\000ab5cb0f09d544f320228314c636ed29a|H|row_1\000sexe|"));
      Assert.assertTrue(items.contains("dataset\000af6196f63905efa61c2d1376b8482eae|H|row_1\000code_postal|"));
      Assert.assertTrue(items.contains("dataset\000f0790ca2c49879179504e93e9e1868de|H|row_1\000ville|"));
      Assert.assertTrue(items.contains("dataset\000f795e514e5b3f452b5a047a574c445e3|H|row_1\000rue|"));
    }
  }

  @Test
  public void testQueryWithWildcards() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    Assert.assertTrue(dataStore.persist("dataset", "row_1", Data.json5()));

    dataStore.flush();

    // Check BlobStore
    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());
    }

    // Check TermStore
    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(21, terms.size());

      List<String> items = terms.stream().map(
              t -> t.getKey().getRow().toString() + SEPARATOR_PIPE + t.getKey().getColumnFamily().toString()
                  + SEPARATOR_PIPE + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE + t.getValue().toString())
          .collect(Collectors.toList());

      // Check hashes
      Assert.assertTrue(items.contains("dataset\0008995d3edb756b461812cdb3501a74032|H|row_1\000path|"));
    }

    // Check queries
    @Var Set<Term> terms = dataStore.termStore().terms(auths, "dataset", "myfile").toSet();

    Assert.assertEquals(1, terms.size());

    terms = dataStore.termStore().terms(auths, "dataset", "csv").toSet();

    Assert.assertEquals(1, terms.size());

    terms = dataStore.termStore().terms(auths, "dataset", Sets.newHashSet("path"), "myfile", null).toSet();

    Assert.assertEquals(1, terms.size());

    terms = dataStore.termStore().terms(auths, "dataset", Sets.newHashSet("path"), "csv", null).toSet();

    Assert.assertEquals(1, terms.size());

    // Query
    @Var AbstractNode node = QueryBuilder.build("myfile.csv");

    @Var Set<String> docsIds = node.execute(dataStore, auths, "dataset").toSet();

    Assert.assertEquals(1, docsIds.size());

    node = QueryBuilder.build("path:myfile.csv");

    docsIds = node.execute(dataStore, auths, "dataset").toSet();

    Assert.assertEquals(1, docsIds.size());

    node = QueryBuilder.build("path:*myfile.csv");

    docsIds = node.execute(dataStore, auths, "dataset").toSet();

    Assert.assertEquals(1, docsIds.size());

    node = QueryBuilder.build("path:\"myfile\" AND path:\"csv\"");

    docsIds = node.execute(dataStore, auths, "dataset").toSet();

    Assert.assertEquals(1, docsIds.size());
  }

  @Test
  public void testQueryWithBloomFilter() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    Assert.assertTrue(dataStore.persist("dataset", "row_1", Data.json5()));

    dataStore.flush();

    // Query
    @Var AbstractNode node = QueryBuilder.build("path:\"myfile.csv\"");

    @Var Set<String> docsIds = node.execute(dataStore, auths, "dataset").toSet();

    Assert.assertEquals(0, docsIds.size());

    node = QueryBuilder.build("path:\"myfile.csv\"");

    docsIds = node.execute(dataStore, auths, "dataset", null, StringCodec::defaultTokenizer).toSet();

    Assert.assertEquals(1, docsIds.size());
  }

  private DataStore newDataStore(Authorizations auths) throws Exception {
    return newDataStore(auths, nextUsername());
  }

  private DataStore newDataStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    DataStore dataStore = new DataStore(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.grantReadPermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.grantReadPermissionOnTermStore(username));
    Assert.assertTrue(dataStore.grantReadPermissionOnCache(username));
    Assert.assertTrue(dataStore.grantWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.grantWritePermissionOnTermStore(username));
    Assert.assertTrue(dataStore.grantWritePermissionOnCache(username));

    return dataStore;
  }
}
