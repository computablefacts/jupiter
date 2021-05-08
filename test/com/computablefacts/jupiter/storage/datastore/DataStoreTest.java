package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.AUTH_ADM;
import static com.computablefacts.jupiter.storage.Constants.TEXT_CACHE;
import static com.computablefacts.jupiter.storage.Constants.TEXT_HASH_INDEX;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Data;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class DataStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void addLocalityGroup() throws Exception {

    Authorizations auths = new Authorizations("ADM");
    DataStore dataStore = newDataStore(auths);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
    }

    // Check TermStore locality groups
    @Var
    Map<String, Set<Text>> groupsBefore =
        Tables.getLocalityGroups(dataStore.termStore().configurations().tableOperations(),
            dataStore.termStore().tableName());

    Assert.assertEquals(0, groupsBefore.size());

    // Check BlobStore locality groups
    groupsBefore =
        Tables.getLocalityGroups(dataStore.blobStore().configurations().tableOperations(),
            dataStore.blobStore().tableName());

    Assert.assertEquals(2, groupsBefore.size());
    Assert.assertTrue(groupsBefore.containsKey(TEXT_CACHE.toString()));
    Assert.assertTrue(groupsBefore.containsKey(TEXT_HASH_INDEX.toString()));

    // Add new locality groups
    Assert.assertTrue(dataStore.addLocalityGroup("dataset_1"));

    // Check TermStore locality groups
    @Var
    Map<String, Set<Text>> groupsAfter =
        Tables.getLocalityGroups(dataStore.termStore().configurations().tableOperations(),
            dataStore.termStore().tableName());

    Assert.assertEquals(9, groupsAfter.size());
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_DB")), groupsAfter.get("dataset_1_DB"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_DT")), groupsAfter.get("dataset_1_DT"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_LU")), groupsAfter.get("dataset_1_LU"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_TT")), groupsAfter.get("dataset_1_TT"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_VIZ")),
        groupsAfter.get("dataset_1_VIZ"));

    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_FCNT")),
        groupsAfter.get("dataset_1_FCNT"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_BCNT")),
        groupsAfter.get("dataset_1_BCNT"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_FIDX")),
        groupsAfter.get("dataset_1_FIDX"));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1_BIDX")),
        groupsAfter.get("dataset_1_BIDX"));

    // Check BlobStore locality groups
    groupsAfter = Tables.getLocalityGroups(dataStore.blobStore().configurations().tableOperations(),
        dataStore.blobStore().tableName());

    Assert.assertEquals(3, groupsAfter.size());
    Assert.assertTrue(groupsAfter.containsKey(TEXT_CACHE.toString()));
    Assert.assertTrue(groupsAfter.containsKey(TEXT_HASH_INDEX.toString()));
    Assert.assertEquals(Sets.newHashSet(new Text("dataset_1")), groupsAfter.get("dataset_1"));
  }

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

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_2", "row_1", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("doe");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(1));
      }
    }

    Assert.assertTrue(dataStore.truncate());

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertTrue(docsIds.isEmpty()); // because the cache has been trashed
      }
    }
  }

  @Test
  public void testRemoveDataset() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_2", "row_1", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("doe");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(1));
      }
    }

    Assert.assertTrue(dataStore.remove("dataset_1"));

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size()); // because the cache has not been trashed
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(1));
      }
    }
  }

  @Test
  public void testAndQuery() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_2", "row_1", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("joh? AND doe");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_2").forEachRemaining(docsIds::add);

        Assert.assertTrue(docsIds.isEmpty());
      }
    }
  }

  @Test
  public void testAndNotQuery() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_2", "row_1", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("doe AND NOT jan?");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_2").forEachRemaining(docsIds::add);

        Assert.assertTrue(docsIds.isEmpty());
      }
    }
  }

  @Test
  public void testOrQuery() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_2", "row_1", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(1));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_2").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_2"), docsIds.get(0));
      }
    }
  }

  @Test
  public void testRunRowLevelAuthorizationsOneHundredTimes() throws Exception {
    for (int i = 0; i < 100; i++) {
      testRowLevelAuthorizations();
    }
  }

  @Test
  public void testRowLevelAuthorizations() throws Exception {

    DataStore dataStore =
        newDataStore(new Authorizations("ADM", "DATASET_1_ROW_1", "DATASET_1_ROW_2"));

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
    }

    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    try (Scanners scanners = dataStore.scanners(new Authorizations("DATASET_1_ROW_1"))) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
      }
    }

    try (Scanners scanners = dataStore.scanners(new Authorizations("DATASET_1_ROW_2"))) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(0));
      }
    }
  }

  @Test
  public void testCount() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
    }

    @Var
    AbstractNode query = QueryBuilder.build("joh* OR jan*");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("*ohn OR *ane");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("joh* AND jan*");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("*ohn AND *ane");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("joh* AND NOT jan*");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("*ohn AND NOT *ane");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, "dataset_1"));
    }
  }

  @Test
  public void testRangeCount() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
    }

    @Var
    AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("age:[15 TO 20]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    query = QueryBuilder.build("age:[15 TO *]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(2, query.cardinality(dataStore, scanners, "dataset_1"));
    }

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, null));
      Assert.assertEquals(1, query.cardinality(dataStore, scanners, "dataset_1"));
    }
  }

  @Test
  public void testRangeQuery() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
    }

    @Var
    AbstractNode query = QueryBuilder.build("age:[* TO 20]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));
      }
    }

    query = QueryBuilder.build("age:[15 TO 20]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));
      }
    }

    query = QueryBuilder.build("age:[15 TO *]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(2, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_2", "dataset_1"), docsIds.get(1));
      }
    }

    // Min. term should be included and max. term excluded
    query = QueryBuilder.build("age:[17 TO 18]");

    try (Scanners scanners = dataStore.scanners(AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        List<Map.Entry<String, String>> docsIds = new ArrayList<>();
        query.execute(dataStore, scanners, writers, null).forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));

        docsIds.clear();
        query.execute(dataStore, scanners, writers, "dataset_1").forEachRemaining(docsIds::add);

        Assert.assertEquals(1, docsIds.size());
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("row_1", "dataset_1"), docsIds.get(0));
      }
    }
  }

  @Test
  public void testDataStoreInfos() throws Exception {

    DataStore dataStore = newDataStore(AUTH_ADM);

    try (Writers writers = dataStore.writers()) {
      dataStore.beginIngest();
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_2", Data.json3(1)));
      dataStore.endIngest(writers, "dataset_1");
    }

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
    map.put("nb_distinct_buckets", 2.0);
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
    map.put("nb_distinct_buckets", 2.0);
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
    map.put("nb_distinct_buckets", 2.0);
    map.put("top_terms_no_false_positives",
        Lists.newArrayList(ImmutableMap.of("term", "1", "nb_occurrences", 2)));
    map.put("top_terms_no_false_negatives",
        Lists.newArrayList(ImmutableMap.of("term", "1", "nb_occurrences", 2)));
    map.put("visibility_labels", Sets.newHashSet("ADM", "DATASET_1_ID"));
    map.put("types", Sets.newHashSet("TEXT"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "dataset_1");
    map.put("field", "first_name");
    map.put("nb_distinct_terms", 2.0);
    map.put("nb_distinct_buckets", 2.0);
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
    map.put("nb_distinct_buckets", 2.0);
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
    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.persist(writers, "dataset_1", "row_1", Data.json2(1)));
    }

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(26, terms.size());

      List<Map.Entry<Key, Value>> bcnt =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_BCNT"))
              .collect(Collectors.toList());

      Assert.assertEquals(3, bcnt.size());
      Assert.assertEquals(3,
          bcnt.stream().mapToInt(kv -> Integer.parseInt(kv.getValue().toString(), 10)).sum());

      List<Map.Entry<Key, Value>> fcnt =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_FCNT"))
              .collect(Collectors.toList());

      Assert.assertEquals(5, fcnt.size());
      Assert.assertEquals(5,
          fcnt.stream().mapToInt(kv -> Integer.parseInt(kv.getValue().toString(), 10)).sum());

      List<Map.Entry<Key, Value>> bidx =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_BIDX"))
              .collect(Collectors.toList());

      Assert.assertEquals(3, bidx.size());

      List<Map.Entry<Key, Value>> fidx =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_FIDX"))
              .collect(Collectors.toList());

      Assert.assertEquals(5, fidx.size());
    }

    // Remove all terms
    try (BatchDeleter deleter = dataStore.termStore().deleter(AUTH_ADM)) {
      Assert.assertTrue(dataStore.termStore().removeDataset(deleter, "dataset_1"));
    }

    // Reindex -> do not update blobs but rebuild the whole terms index
    try (Writers writers = dataStore.writers()) {
      Assert.assertTrue(dataStore.reindex(writers, "dataset_1", "row_1", Data.json2(1)));
    }

    try (Scanner scanner = dataStore.blobStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());
    }

    try (Scanner scanner = dataStore.termStore().scanner(auths)) {

      List<Map.Entry<Key, Value>> terms = new ArrayList<>();
      scanner.iterator().forEachRemaining(terms::add);

      Assert.assertEquals(26, terms.size());

      List<Map.Entry<Key, Value>> bcnt =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_BCNT"))
              .collect(Collectors.toList());

      Assert.assertEquals(3, bcnt.size());
      Assert.assertEquals(3,
          bcnt.stream().mapToInt(kv -> Integer.parseInt(kv.getValue().toString(), 10)).sum());

      List<Map.Entry<Key, Value>> fcnt =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_FCNT"))
              .collect(Collectors.toList());

      Assert.assertEquals(5, fcnt.size());
      Assert.assertEquals(5,
          fcnt.stream().mapToInt(kv -> Integer.parseInt(kv.getValue().toString(), 10)).sum());

      List<Map.Entry<Key, Value>> bidx =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_BIDX"))
              .collect(Collectors.toList());

      Assert.assertEquals(3, bidx.size());

      List<Map.Entry<Key, Value>> fidx =
          terms.stream().filter(kv -> kv.getKey().getColumnFamily().toString().endsWith("_FIDX"))
              .collect(Collectors.toList());

      Assert.assertEquals(5, fidx.size());
    }
  }

  private DataStore newDataStore(Authorizations auths) throws Exception {

    String username = nextUsername();
    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    DataStore dataStore = new DataStore(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.grantReadPermissions(username));
    Assert.assertTrue(dataStore.grantWritePermissionOnBlobStore(username));
    Assert.assertTrue(dataStore.grantWritePermissionOnTermStore(username));

    return dataStore;
  }
}
