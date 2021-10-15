package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.AUTH_ADM;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.jupiter.storage.termstore.TermStore.BACKWARD_INDEX;
import static com.computablefacts.jupiter.storage.termstore.TermStore.FORWARD_INDEX;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Data;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

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

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    AbstractNode query = QueryBuilder.build("doe");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(0));

    Assert.assertTrue(dataStore.truncate());

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertTrue(docsIds.isEmpty()); // because the cache has been trashed

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertTrue(docsIds.isEmpty()); // because the cache has been trashed
  }

  @Test
  public void testRemoveDataset() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    AbstractNode query = QueryBuilder.build("doe");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(0));

    Assert.assertTrue(dataStore.remove("dataset_1"));

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(0, docsIds.size());

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(0));
  }

  @Test
  public void testAndQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh? AND doe");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertTrue(docsIds.isEmpty());
  }

  @Test
  public void testAndNotQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("doe AND NOT jan?");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertTrue(docsIds.isEmpty());
  }

  @Test
  public void testOrQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_2", "row_1", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    docsIds.clear();
    query.execute(dataStore, "dataset_2").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(0));
  }

  @Test
  public void testRowLevelAuthorizations() throws Exception {

    String username = nextUsername();
    DataStore dataStore =
        newDataStore(new Authorizations("ADM", "DATASET_1_ROW_1", "DATASET_1_ROW_2"), username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    dataStore.setBlobProcessor(
        new AccumuloBlobProcessor(dataStore.blobStore(), new Authorizations("DATASET_1_ROW_1")));
    dataStore.setTermProcessor(
        new AccumuloTermProcessor(dataStore.termStore(), new Authorizations("DATASET_1_ROW_1")));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

    dataStore.setBlobProcessor(
        new AccumuloBlobProcessor(dataStore.blobStore(), new Authorizations("DATASET_1_ROW_2")));
    dataStore.setTermProcessor(
        new AccumuloTermProcessor(dataStore.termStore(), new Authorizations("DATASET_1_ROW_2")));

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(0));
  }

  @Test
  public void testCount() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var
    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    Assert.assertEquals(2, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("*ohn OR *ane");

    Assert.assertEquals(2, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("joh* AND jan*");

    Assert.assertEquals(1, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("*ohn AND *ane");

    Assert.assertEquals(1, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("joh* AND NOT jan*");

    Assert.assertEquals(1, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("*ohn AND NOT *ane");

    Assert.assertEquals(1, query.cardinality(dataStore, "dataset_1"));
  }

  @Test
  public void testRangeCount() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var
    AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    Assert.assertEquals(2, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("age:[15 TO 20]");

    Assert.assertEquals(2, query.cardinality(dataStore, "dataset_1"));

    query = QueryBuilder.build("age:[15 TO *]");

    Assert.assertEquals(2, query.cardinality(dataStore, "dataset_1"));

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    Assert.assertEquals(1, query.cardinality(dataStore, "dataset_1"));
  }

  @Test
  public void testRangeQuery() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    @Var
    AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    List<Map.Entry<String, String>> docsIds = new ArrayList<>();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

    query = QueryBuilder.build("age:[15 TO 20]");

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

    query = QueryBuilder.build("age:[15 TO *]");

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    docsIds.clear();
    query.execute(dataStore, "dataset_1").forEachRemaining(docsIds::add);

    Assert.assertEquals(1, docsIds.size());
    Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
  }

  @Test
  public void testMatchValue() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), AUTH_ADM));

    List<String> docsIds = new ArrayList<>();
    dataStore.matchValue("dataset_1", "Actors[*]¤name", "Tom Cruise")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchValue("dataset_1", "Actors[*]¤weight", 67.5).forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchValue("dataset_1", "Actors[*]¤age", 73).forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchValue("dataset_1", "Actors[*]¤hasGreyHair", false)
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchValue("dataset_1", "Actors[*]¤hasChildren", true).forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testMatchHash() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), null));

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json(2)));

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), AUTH_ADM));

    List<String> docsIds = new ArrayList<>();
    dataStore.matchHash("dataset_1", "Actors[*]¤name", "8f8a04ea49585975fcf1e452b988e085")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchHash("dataset_1", "Actors[*]¤weight", "4103e8509cbdf6b3372222061bbe1da6")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchHash("dataset_1", "Actors[*]¤age", "3974c437d717863985a0b5618f289b46")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchHash("dataset_1", "Actors[*]¤hasGreyHair", "e495b7e5056dbfc4e854950696d4c3cc")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));

    docsIds.clear();
    dataStore.matchHash("dataset_1", "Actors[*]¤hasChildren", "5db32d6ecc1f5ef816ebe6268a3343c2")
        .forEachRemaining(docsIds::add);

    Assert.assertEquals(2, docsIds.size());
    Assert.assertTrue(docsIds.contains("row_1"));
    Assert.assertTrue(docsIds.contains("row_2"));
  }

  @Test
  public void testDataStoreInfos() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(AUTH_ADM, username);

    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), AUTH_ADM));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), AUTH_ADM));

    dataStore.beginIngest();
    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.persist("dataset_1", "row_2", Data.json3(1)));
    dataStore.endIngest("dataset_1");

    dataStore.flush();

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    DataStoreInfos infos = dataStore.infos(Sets.newHashSet("dataset_1"), AUTH_ADM);
    Map<String, Object> json = infos.json();

    List<Map<String, Object>> jsons =
        ((List<Map<String, Object>>) json.get("fields")).stream().peek(map -> {
          Assert.assertTrue(
              WildcardMatcher.match((String) map.get("last_update"), "????-??-??T??:??:??*Z"));
          map.remove("last_update");
        }).collect(Collectors.toList());

    Assert.assertEquals(5, jsons.size());

    Map<String, Object> map = new HashMap<>();
    map.put("dataset", "dataset_1");
    map.put("field", "birthdate");
    map.put("nb_distinct_terms", 2.0);
    map.put("nb_distinct_buckets", 2L);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "2004-04-01T00:00:00Z", "nb_occurrences", 1),
            ImmutableMap.of("term", "2003-04-01T00:00:00Z", "nb_occurrences", 1)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "2004-04-01T00:00:00Z", "nb_occurrences", 1),
            ImmutableMap.of("term", "2003-04-01T00:00:00Z", "nb_occurrences", 1)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_BIRTHDATE"));
    map.put("types", Sets.newHashSet("DATE"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "dataset_1");
    map.put("field", "last_name");
    map.put("nb_distinct_terms", 1.0);
    map.put("nb_distinct_buckets", 2L);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "doe", "nb_occurrences", 2)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "doe", "nb_occurrences", 2)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_LAST_NAME"));
    map.put("types", Sets.newHashSet("TEXT"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "dataset_1");
    map.put("field", "id");
    map.put("nb_distinct_terms", 1.0);
    map.put("nb_distinct_buckets", 2L);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "1", "nb_occurrences", 2)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "1", "nb_occurrences", 2)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_ID"));
    map.put("types", Sets.newHashSet("NUMBER"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "dataset_1");
    map.put("field", "first_name");
    map.put("nb_distinct_terms", 2.0);
    map.put("nb_distinct_buckets", 2L);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "john", "nb_occurrences", 1),
            ImmutableMap.of("term", "jane", "nb_occurrences", 1)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "john", "nb_occurrences", 1),
            ImmutableMap.of("term", "jane", "nb_occurrences", 1)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_FIRST_NAME"));
    map.put("types", Sets.newHashSet("TEXT"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "dataset_1");
    map.put("field", "age");
    map.put("nb_distinct_terms", 2.0);
    map.put("nb_distinct_buckets", 2L);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "17", "nb_occurrences", 1),
            ImmutableMap.of("term", "18", "nb_occurrences", 1)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "17", "nb_occurrences", 1),
            ImmutableMap.of("term", "18", "nb_occurrences", 1)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_AGE"));
    map.put("types", Sets.newHashSet("NUMBER"));

    Assert.assertTrue(jsons.contains(map));
  }

  @Test
  public void testReindex() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), null));

    dataStore.beginIngest();

    Assert.assertTrue(dataStore.persist("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.endIngest("dataset_1"));

    dataStore.flush();

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(6, blobs.size());

      List<String> items = blobs.stream()
          .map(t -> t.getKey().getRow().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnFamily().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE
              + t.getValue().toString())
          .collect(Collectors.toList());

      // Hash index
      Assert.assertTrue(
          items.contains("dataset_1\u00006174693c483abae057d822c6cc4c67b9|ARR43|age|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u00008c979aa1006083b505eadf7fdbbd786c|ARR43|birthdate|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u000088fecf016203005fdbeb018c1376c333|ARR43|first_name|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u00004b5c86196dd52c0cf2673d2d0a569431|ARR43|last_name|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u0000717c7b8afebbfb7137f6f0f99beb2a94|ARR43|id|row_1"));

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset_1\0row_1|JSO||{\"birthdate\":\"2004-04-01T00:00:00Z\",\"last_name\":\"doe\",\"id\":\"1\",\"first_name\":\"john\",\"age\":17}"));
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(39, terms.size());

      List<String> items = terms.stream()
          .map(t -> t.getKey().getRow().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnFamily().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE
              + t.getValue().toString())
          .collect(Collectors.toList());

      // Check all items but the ones in the LU, DT and TT column families
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FCNT|birthdate\0003|1"));
      Assert.assertTrue(
          items.contains("dataset_1\0002004-04-01T00:00:00Z|FIDX|row_1\000birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FCNT|id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FIDX|row_1\000id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FCNT|age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FIDX|row_1\000age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000age\0002|VIZ||ADM\000DATASET_1_AGE"));
      Assert.assertTrue(items.contains("dataset_1\000age\0005|DB||1"));
      Assert
          .assertTrue(items.contains("dataset_1\000birthdate\0003|VIZ||ADM\0DATASET_1_BIRTHDATE"));
      Assert.assertTrue(items.contains("dataset_1\000birthdate\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(
          items.contains("dataset_1\000first_name\0001|VIZ||DATASET_1_FIRST_NAME\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000first_name\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000id\0002|VIZ||DATASET_1_ID\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000id\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FIDX|row_1\000first_name\0001|1"));
      Assert.assertTrue(
          items.contains("dataset_1\000last_name\0001|VIZ||DATASET_1_LAST_NAME\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000last_name\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BIDX|row_1\000first_name\0001|1"));
    }

    // Remove all terms
    Assert.assertTrue(dataStore.termStore().removeDataset("dataset_1"));

    // Reindex -> do not update blobs but rebuild the whole terms index
    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), null));

    dataStore.beginIngest();

    Assert.assertTrue(dataStore.reindex("dataset_1", "row_1", Data.json2(1)));
    Assert.assertTrue(dataStore.endIngest("dataset_1"));

    dataStore.flush();

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(6, blobs.size());

      List<String> items = blobs.stream()
          .map(t -> t.getKey().getRow().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnFamily().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE
              + t.getValue().toString())
          .collect(Collectors.toList());

      // Hash index
      Assert.assertTrue(
          items.contains("dataset_1\u00006174693c483abae057d822c6cc4c67b9|ARR43|age|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u00008c979aa1006083b505eadf7fdbbd786c|ARR43|birthdate|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u000088fecf016203005fdbeb018c1376c333|ARR43|first_name|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u00004b5c86196dd52c0cf2673d2d0a569431|ARR43|last_name|row_1"));
      Assert.assertTrue(
          items.contains("dataset_1\u0000717c7b8afebbfb7137f6f0f99beb2a94|ARR43|id|row_1"));

      // Raw data
      Assert.assertTrue(items.contains(
          "dataset_1\0row_1|JSO||{\"birthdate\":\"2004-04-01T00:00:00Z\",\"last_name\":\"doe\",\"id\":\"1\",\"first_name\":\"john\",\"age\":17}"));
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(39, terms.size());

      List<String> items = terms.stream()
          .map(t -> t.getKey().getRow().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnFamily().toString() + SEPARATOR_PIPE
              + t.getKey().getColumnQualifier().toString() + SEPARATOR_PIPE
              + t.getValue().toString())
          .collect(Collectors.toList());

      // Check all items but the ones in the LU, DT and TT column families
      Assert.assertTrue(items.contains("dataset_1\0002004-04-01T00:00:00Z|FCNT|birthdate\0003|1"));
      Assert.assertTrue(
          items.contains("dataset_1\0002004-04-01T00:00:00Z|FIDX|row_1\000birthdate\0003|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FCNT|id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000?1*|FIDX|row_1\000id\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FCNT|age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000??217*|FIDX|row_1\000age\0002|1"));
      Assert.assertTrue(items.contains("dataset_1\000age\0002|VIZ||ADM\000DATASET_1_AGE"));
      Assert.assertTrue(items.contains("dataset_1\000age\0005|DB||1"));
      Assert
          .assertTrue(items.contains("dataset_1\000birthdate\0003|VIZ||ADM\0DATASET_1_BIRTHDATE"));
      Assert.assertTrue(items.contains("dataset_1\000birthdate\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000doe|FIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BCNT|last_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000eod|BIDX|row_1\000last_name\0001|1"));
      Assert.assertTrue(
          items.contains("dataset_1\000first_name\0001|VIZ||DATASET_1_FIRST_NAME\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000first_name\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000id\0002|VIZ||DATASET_1_ID\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000id\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000john|FIDX|row_1\000first_name\0001|1"));
      Assert.assertTrue(
          items.contains("dataset_1\000last_name\0001|VIZ||DATASET_1_LAST_NAME\000ADM"));
      Assert.assertTrue(items.contains("dataset_1\000last_name\0005|DB||1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BCNT|first_name\0001|1"));
      Assert.assertTrue(items.contains("dataset_1\000nhoj|BIDX|row_1\000first_name\0001|1"));
    }
  }

  @Test
  public void testIndexNumbersPrefixedWithZeroes() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    // Index
    dataStore.setBlobProcessor(new AccumuloBlobProcessor(dataStore.blobStore(), null));
    dataStore.setTermProcessor(new AccumuloTermProcessor(dataStore.termStore(), null));
    dataStore.setHashProcessor(new AccumuloHashProcessor(dataStore.blobStore(), null));

    dataStore.beginIngest();

    Assert.assertTrue(dataStore.persist("dataset", "row_1", Data.json4()));
    Assert.assertTrue(dataStore.endIngest("dataset"));

    dataStore.flush();

    // Check BlobStore
    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> blobs = new ArrayList<>();
      scanner.iterator().forEachRemaining(blobs::add);

      Assert.assertEquals(11, blobs.size());

      boolean hasId = blobs.stream().anyMatch(blob -> {

        String row = blob.getKey().getRow().toString();
        String cf = blob.getKey().getColumnFamily().toString();
        String cq = blob.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u0000365ccfea623eaebf17f36c5a0cdc4ddc" /* 0001 */)
            && cf.equals(BlobStore.arrayShard("row_1")) && cq.equals("id");
      });

      Assert.assertTrue(hasId);

      boolean hasCodePostal = blobs.stream().anyMatch(blob -> {

        String row = blob.getKey().getRow().toString();
        String cf = blob.getKey().getColumnFamily().toString();
        String cq = blob.getKey().getColumnQualifier().toString();

        return row.equals("dataset\0af6196f63905efa61c2d1376b8482eae" /* 01800 */)
            && cf.equals(BlobStore.arrayShard("row_1")) && cq.equals("code_postal");
      });

      Assert.assertTrue(hasCodePostal);

      boolean hasNbConnexions = blobs.stream().anyMatch(blob -> {

        String row = blob.getKey().getRow().toString();
        String cf = blob.getKey().getColumnFamily().toString();
        String cq = blob.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u000080a346d5bedec92a095e873ce5e98d3a" /* 0 */)
            && cf.equals(BlobStore.arrayShard("row_1")) && cq.equals("nb_connexions");
      });

      Assert.assertTrue(hasNbConnexions);
    }

    // Check TermStore
    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(96, terms.size());

      @Var
      boolean hasId = terms.stream().anyMatch(term -> {

        String row = term.getKey().getRow().toString();
        String cf = term.getKey().getColumnFamily().toString();
        String cq = term.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u00000001" /* 0001 */) && cf.equals(FORWARD_INDEX)
            && cq.equals("row_1\0id\u00001");
      });

      Assert.assertTrue(hasId);

      hasId = terms.stream().anyMatch(term -> {

        String row = term.getKey().getRow().toString();
        String cf = term.getKey().getColumnFamily().toString();
        String cq = term.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u00001000" /* 0001 */) && cf.equals(BACKWARD_INDEX)
            && cq.equals("row_1\0id\u00001");
      });

      Assert.assertTrue(hasId);

      @Var
      boolean hasCodePostal = terms.stream().anyMatch(term -> {

        String row = term.getKey().getRow().toString();
        String cf = term.getKey().getColumnFamily().toString();
        String cq = term.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u000001800" /* 01800 */) && cf.equals(FORWARD_INDEX)
            && cq.equals("row_1\0code_postal\u00001");
      });

      Assert.assertTrue(hasCodePostal);

      hasCodePostal = terms.stream().anyMatch(term -> {

        String row = term.getKey().getRow().toString();
        String cf = term.getKey().getColumnFamily().toString();
        String cq = term.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u000000810" /* 01800 */) && cf.equals(BACKWARD_INDEX)
            && cq.equals("row_1\0code_postal\u00001");
      });

      Assert.assertTrue(hasCodePostal);

      boolean hasNbConnexions = terms.stream().anyMatch(term -> {

        String row = term.getKey().getRow().toString();
        String cf = term.getKey().getColumnFamily().toString();
        String cq = term.getKey().getColumnQualifier().toString();

        return row.equals("dataset\u0000?0*" /* 0 */) && cf.equals(FORWARD_INDEX)
            && cq.equals("row_1\0nb_connexions\u00002");
      });

      Assert.assertTrue(hasNbConnexions);
    }
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
