package com.computablefacts.jupiter.storage.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class DataStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testAddLocalityGroup() {
    // TODO
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    DataStore dataStore = new DataStore(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());

    Assert.assertTrue(dataStore.create()); // ensure create is reentrant
    Assert.assertTrue(dataStore.isReady());
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    DataStore dataStore = new DataStore(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());

    Assert.assertTrue(dataStore.destroy());
    Assert.assertFalse(dataStore.isReady());

    Assert.assertTrue(dataStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(dataStore.isReady());
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testReadBlobStoreWithoutTablesPermissions() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    String username = nextUsername();
    DataStore dataStore = newDataStore(auths, username);

    Assert.assertTrue(dataStore.revokeReadPermissions(username));

    try (Scanners scanners = dataStore.scanners(auths)) {

      ScannerBase scanner = scanners.blob();
      scanner.fetchColumnFamily(new Text("first_dataset"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testReadTermStoreWithoutTablesPermissions() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    String username = nextUsername();
    DataStore dataStore = newDataStore(auths, username);

    Assert.assertTrue(dataStore.revokeReadPermissions(username));

    try (Scanners scanners = dataStore.scanners(auths)) {

      ScannerBase scanner = scanners.index();
      scanner.fetchColumnFamily(new Text("first_dataset_FIDX"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here
    }
  }

  @Test
  public void testReadBlobStoreWithTablesPermissions() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    String username = nextUsername();
    DataStore dataStore = newDataStore(auths, username);

    try (Scanners scanners = dataStore.scanners(auths)) {

      ScannerBase scanner = scanners.blob();
      scanner.fetchColumnFamily(new Text("first_dataset"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());
    }
  }

  @Ignore
  @Test
  public void testReadTermStoreWithTablesPermissions() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    String username = nextUsername();
    DataStore dataStore = newDataStore(auths, username);

    try (Scanners scanners = dataStore.scanners(auths)) {

      ScannerBase scanner = scanners.index();
      scanner.fetchColumnFamily(new Text("first_dataset_FIDX"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add);

      Assert.assertEquals(120, list.size());
    }
  }

  @Test
  public void testGrantWriteOnBlobStoreThenRevoke() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(Constants.AUTH_ADM, username);

    try (Writers writers = dataStore.writers()) {

      Mutation m = new Mutation("test_row");
      m.put(new Text("first_dataset"), Constants.TEXT_EMPTY, Constants.VALUE_EMPTY);
      writers.blob().addMutation(m);

      Assert.assertTrue(writers.flush());
    }

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore(username));

    try (Writers writers = dataStore.writers()) {

      Mutation m = new Mutation("test_row");
      m.put(new Text("second_dataset"), Constants.TEXT_EMPTY, Constants.VALUE_EMPTY);
      writers.blob().addMutation(m);

      Assert.assertFalse(writers.flush());
    }
  }

  @Test
  public void testGrantWriteOnTermStoreThenRevoke() throws Exception {

    String username = nextUsername();
    DataStore dataStore = newDataStore(Constants.AUTH_ADM, username);

    try (Writers writers = dataStore.writers()) {

      Mutation m = new Mutation("test");
      m.put(new Text("first_dataset_FIDX"), new Text("000|0000-00-00T00:00:00.000Z\0field"),
          Constants.VALUE_EMPTY);
      writers.index().addMutation(m);

      Assert.assertTrue(writers.flush());
    }

    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore(username));

    try (Writers writers = dataStore.writers()) {

      Mutation m = new Mutation("test");
      m.put(new Text("second_dataset_FIDX"), new Text("000|0000-00-00T00:00:00.000Z\0field"),
          Constants.VALUE_EMPTY);
      writers.index().addMutation(m);

      Assert.assertFalse(writers.flush());
    }
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = Constants.AUTH_ADM;
    DataStore dataStore = newDataStore(auths);

    List<Term> terms = new ArrayList<>();

    try (Scanners scanners = dataStore.scanners(auths)) {

      dataStore.numericalRangeScan(scanners, "first_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(10, terms.size());

      terms.forEach(term -> {
        Assert.assertEquals("Actors[*]¤age", term.field());
        Assert.assertEquals("56", term.term());
        Assert.assertTrue(term.isNumber());
      });

      terms.clear();

      dataStore.numericalRangeScan(scanners, "second_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(10, terms.size());

      terms.forEach(term -> {
        Assert.assertEquals("Actors[*]¤age", term.field());
        Assert.assertEquals("56", term.term());
        Assert.assertTrue(term.isNumber());
      });

      terms.clear();

      dataStore.numericalRangeScan(scanners, "third_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(10, terms.size());

      terms.forEach(term -> {
        Assert.assertEquals("Actors[*]¤age", term.field());
        Assert.assertEquals("56", term.term());
        Assert.assertTrue(term.isNumber());
      });
    }

    Assert.assertTrue(dataStore.remove("first_dataset"));
    Assert.assertTrue(dataStore.remove("second_dataset"));

    try (Scanners scanners = dataStore.scanners(auths)) {

      terms.clear();

      dataStore.numericalRangeScan(scanners, "first_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(0, terms.size());

      terms.clear();

      dataStore.numericalRangeScan(scanners, "second_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(0, terms.size());

      terms.clear();

      dataStore.numericalRangeScan(scanners, "third_dataset", 50, 60).forEachRemaining(terms::add);

      Assert.assertEquals(10, terms.size());

      terms.forEach(term -> {
        Assert.assertEquals("Actors[*]¤age", term.field());
        Assert.assertEquals("56", term.term());
        Assert.assertTrue(term.isNumber());
      });
    }
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9",
        "SECOND_DATASET_ROW_0", "SECOND_DATASET_ROW_1", "SECOND_DATASET_ROW_2",
        "SECOND_DATASET_ROW_3", "SECOND_DATASET_ROW_4", "SECOND_DATASET_ROW_5",
        "SECOND_DATASET_ROW_6", "SECOND_DATASET_ROW_7", "SECOND_DATASET_ROW_8",
        "SECOND_DATASET_ROW_9", "THIRD_DATASET_ROW_0", "THIRD_DATASET_ROW_1", "THIRD_DATASET_ROW_2",
        "THIRD_DATASET_ROW_3", "THIRD_DATASET_ROW_4", "THIRD_DATASET_ROW_5", "THIRD_DATASET_ROW_6",
        "THIRD_DATASET_ROW_7", "THIRD_DATASET_ROW_8", "THIRD_DATASET_ROW_9", "FIRST_DATASET_CNT",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT",
        "THIRD_DATASET_VIZ");
    DataStore dataStore = newDataStore(auths);

    // BlobStore
    // TODO

    // Test TermStore's metadata
    // TODO

    // Test TermStore's terms
    // TODO

    Assert.assertTrue(dataStore.truncate());

    // BlobStore
    // TODO

    // Test TermStore's metadata
    // TODO

    // Test TermStore's terms
    // TODO
  }

  @Test
  public void testJsonScan() throws Exception {

    Authorizations auths1 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    Authorizations auths2 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4");
    DataStore dataStore = newDataStore(auths1);

    // Use the Scanner auths to filter data
    Set<Blob<Value>> list1 = new HashSet<>();

    try (Scanners scanners = dataStore.scanners(auths2)) { // keep order

      Iterator<Blob<Value>> iterator =
          dataStore.jsonScan(scanners, "first_dataset", Sets.newHashSet());

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list1.add(blob);
        Assert.assertEquals("row_" + (list1.size() - 1), blob.key());
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", blob.value().toString());
      }
    }

    // Use the doc ids to filter data
    Set<Blob<Value>> list2 = new HashSet<>();

    try (
        Scanners scanners = new Scanners(dataStore.configurations(), dataStore.name(), auths1, 2)) { // out-of-order

      Iterator<Blob<Value>> iterator = dataStore.jsonScan(scanners, "first_dataset",
          Sets.newHashSet(), Sets.newHashSet("row_0", "row_1", "row_2", "row_3", "row_4"));

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list2.add(blob);
      }
    }

    Assert.assertEquals(list1, list2);
  }

  @Ignore
  @Test
  public void testSearchTerm() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        List<String> list = new ArrayList<>();
        dataStore
            .searchByTerm(scanners, writers, "first_dataset", DataStore.normalize("Connor"), null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();
        dataStore.searchByTerm(scanners, writers, "first_dataset",
            DataStore.normalize("Isabella Jane"), null, null).forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }
      }
    }
  }

  @Ignore
  @Test
  public void testSearchTerms() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        List<String> list = new ArrayList<>();
        dataStore
            .searchByTerms(scanners, writers, "first_dataset", Sets.newHashSet(
                DataStore.normalize("Connor"), DataStore.normalize("Isabella Jane")), null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }
      }
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {

    Authorizations auths = new Authorizations("FOURTH_DATASET_ROW_0", "FOURTH_DATASET_ROW_1",
        "FOURTH_DATASET_ROW_2", "FOURTH_DATASET_ROW_3", "FOURTH_DATASET_ROW_4",
        "FOURTH_DATASET_ROW_5", "FOURTH_DATASET_ROW_6", "FOURTH_DATASET_ROW_7",
        "FOURTH_DATASET_ROW_8", "FOURTH_DATASET_ROW_9");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        // Create a new dataset
        for (int i = 0; i < 10; i++) {
          Assert.assertTrue(dataStore.persist(writers, "fourth_dataset", "row_" + i, json2(i), null,
              Codecs.defaultTokenizer));
        }

        Assert.assertTrue(writers.flush());

        // Test three simple queries
        List<String> list = new ArrayList<>();
        QueryBuilder.build("uuid:\"5\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(0, list.size()); // triggers "Discard small terms"...

        list.clear();

        QueryBuilder.build("Actors[*]¤uuid:\"item1*\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_1", list.get(0));

        list.clear();

        QueryBuilder.build("Actors[*]¤uuid:\"item5?\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_5", list.get(0));

        list.clear();

        // Test an array query
        QueryBuilder.build("Actors[*]¤children[*]:\"Suri\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();

        // Test an AND query
        QueryBuilder.build(
            "Actors[*]¤children[*]:\"Suri\" AND Actors[*]¤uuid:\"item?0\" AND Actors[*]¤uuid:\"item5?\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_5", list.get(0));

        list.clear();

        // Test an OR query
        QueryBuilder.build(
            "Actors[*]¤children[*]:\"Roel\" AND (Actors[*]¤uuid:\"item4?\" OR Actors[*]¤uuid:\"item5?\")")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("row_4", list.get(0));
        Assert.assertEquals("row_5", list.get(1));

        list.clear();
      }
    }
  }

  @Test
  public void testWriteReadCacheSync() throws Exception {

    DataStore dataStore = newDataStore(Authorizations.EMPTY);

    try (Scanners scanners = dataStore.scanners(Authorizations.EMPTY)) { // keep order
      try (Writers writers = dataStore.writers()) {

        Set<String> set = new HashSet<>();

        for (int i = 0; i < 10; i++) {
          set.add(Integer.toString(i, 10));
        }

        String cacheId = DataStoreCache.write(writers, set.iterator());

        Set<String> setNew = new HashSet<>();
        DataStoreCache.read(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        DataStoreCache.read(scanners, cacheId, "5").forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("5", "6", "7", "8", "9"), setNew);
      }
    }
  }

  @Test
  public void testWriteReadCacheAsync() throws Exception {

    DataStore dataStore = newDataStore(Authorizations.EMPTY);

    try (Scanners scanners = dataStore.scanners(Authorizations.EMPTY)) { // keep order
      try (Writers writers = dataStore.writers()) {

        Set<String> set = new HashSet<>();

        for (int i = 0; i < 10; i++) {
          set.add(Integer.toString(i, 10));
        }

        String cacheId = DataStoreCache.write(writers, set.iterator(), 5);

        Thread.sleep(1000);

        Set<String> setNew = new HashSet<>();
        DataStoreCache.read(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        DataStoreCache.read(scanners, cacheId, "5").forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("5", "6", "7", "8", "9"), setNew);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNumericalRangeScanOpenedBeginOpenedEnd() throws Exception {

    Authorizations auths =
        new Authorizations("FIRST_DATASET_ACTORS", "SECOND_DATASET_ACTORS", "THIRD_DATASET_ACTORS");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", null, null,
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add); // Throws an
                                                                               // exception
    }
  }

  @Test
  public void testNumericalRangeScanClosedBeginClosedEnd() throws Exception {

    Authorizations auths =
        new Authorizations("FIRST_DATASET_ACTORS", "SECOND_DATASET_ACTORS", "THIRD_DATASET_ACTORS");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", 55, 57,
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).bucketId());
      }
    }
  }

  @Test
  public void testNumericalRangeScanClosedBeginOpenedEnd() throws Exception {

    Authorizations auths =
        new Authorizations("FIRST_DATASET_ACTORS", "SECOND_DATASET_ACTORS", "THIRD_DATASET_ACTORS");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", 56, null,
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).bucketId());
      }
    }
  }

  @Test
  public void testNumericalRangeScanOpenedBeginClosedEnd() throws Exception {

    Authorizations auths =
        new Authorizations("FIRST_DATASET_ACTORS", "SECOND_DATASET_ACTORS", "THIRD_DATASET_ACTORS");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", null, 57,
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).bucketId());
      }
    }
  }

  @Ignore
  @Test
  public void testSearchByNumericalRangeClosedBeginClosedEnd() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND Actors[*]¤age:[50 TO 60]");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset");

        Assert.assertEquals(10, query.count(dataStore, scanners, "first_dataset"));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Ignore
  @Test
  public void testSearchByNumericalRangeOpenedBeginClosedEnd() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND Actors[*]¤age:[* TO 60]");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset");

        Assert.assertEquals(10, query.count(dataStore, scanners, "first_dataset"));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testSearchByNumericalRangeClosedBeginOpenedEnd() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND Actors[*]¤age:[50 TO *]");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset");

        Assert.assertEquals(10, query.count(dataStore, scanners, "first_dataset"));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testSearchByNumericalRangeNoResult() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND Actors[0]¤age:[40 TO 50]");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset");

        Assert.assertEquals(0, query.count(dataStore, scanners, "first_dataset"));
        Assert.assertEquals(0, Iterators.size(iterator));
      }
    }
  }

  @Ignore
  @Test
  public void testDataStoreInfos() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    DataStoreInfos infos = dataStore.infos(Sets.newHashSet("first_dataset"), Constants.AUTH_ADM);
    Map<String, Object> json = infos.json();

    List<Map<String, Object>> jsons =
        ((List<Map<String, Object>>) json.get("fields")).stream().peek(map -> {
          Assert.assertTrue(
              WildcardMatcher.match((String) map.get("last_update"), "????-??-??T??:??:??*Z"));
          map.remove("last_update");
        }).collect(Collectors.toList());

    Assert.assertEquals(10, jsons.size());

    Map<String, Object> map = new HashMap<>();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].age");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_AGE"));
    map.put("types", Sets.newHashSet("NUMBER"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].children[*]");
    map.put("nb_index_entries", 30L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_CHILDREN"));
    map.put("types", Sets.newHashSet("STRING"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].hasChildren");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_HASCHILDREN"));
    map.put("types", Sets.newHashSet("BOOLEAN"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].weight");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_WEIGHT"));
    map.put("types", Sets.newHashSet("NUMBER"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].Birthdate");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_BIRTHDATE"));
    map.put("types", Sets.newHashSet("STRING"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].Born At");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_BORN_AT"));
    map.put("types", Sets.newHashSet("STRING"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].name");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_NAME"));
    map.put("types", Sets.newHashSet("STRING"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].photo");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_PHOTO"));
    map.put("types", Sets.newHashSet("STRING"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].hasGreyHair");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_HASGREYHAIR"));
    map.put("types", Sets.newHashSet("BOOLEAN"));

    Assert.assertTrue(jsons.contains(map));

    map.clear();
    map.put("dataset", "first_dataset");
    map.put("field", "Actors[*].uuid");
    map.put("nb_index_entries", 10L);
    map.put("visibility_labels",
        Sets.newHashSet("ADM", "FIRST_DATASET_ACTORS", "FIRST_DATASET_ACTORS_UUID"));
    map.put("types", Sets.newHashSet("NUMBER"));

    Assert.assertTrue(jsons.contains(map));
  }

  private void fillDataStore(DataStore dataStore) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    try (Writers writers = dataStore.writers()) {
      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "first_dataset", "row_" + i, json1(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "second_dataset", "row_" + i, json1(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "third_dataset", "row_" + i, json1(i)));
      }
    }
  }

  private DataStore newDataStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newDataStore(auths, username);
  }

  private DataStore newDataStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    DataStore dataStore = new DataStore(configurations, tableName);

    if (dataStore.create()) {
      fillDataStore(dataStore);
    }

    boolean isOk = dataStore.grantReadPermissions(username);

    return dataStore;
  }

  private String json1(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }]}";
  }

  private String json2(int i) {
    return "{\n" + "  \"uuid\": " + i + ",\n" + "  \"Actors\": [\n" + "    {\n"
        + "      \"uuid\": \"item" + i + "0\",\n" + "      \"name\": \"Tom Cruise\",\n"
        + "      \"age\": 56,\n" + "      \"Born At\": \"Syracuse, NY\",\n"
        + "      \"Birthdate\": \"July 3, 1962\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\",\n"
        + "      \"wife\": null,\n" + "      \"weight\": 67.5,\n" + "      \"hasChildren\": true,\n"
        + "      \"hasGreyHair\": false,\n" + "      \"children\": [\n" + "        \"Suri\",\n"
        + "        \"Isabella Jane\",\n" + "        \"Connor\"\n" + "      ]\n" + "    },\n"
        + "    {\n" + "      \"uuid\": \"item" + i + "1\",\n"
        + "      \"name\": \"Robert Downey Jr.\",\n" + "      \"age\": 53,\n"
        + "      \"Born At\": \"New York City, NY\",\n"
        + "      \"Birthdate\": \"April 4, 1965\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/Robert-Downey-Jr.jpg\",\n"
        + "      \"wife\": \"Susan Downey\",\n" + "      \"weight\": 77.1,\n"
        + "      \"hasChildren\": true,\n" + "      \"hasGreyHair\": false,\n"
        + "      \"children\": [\n" + "        \"Indio Falconer\",\n" + "        \"Avri Roel\",\n"
        + "        \"Exton Elias\"\n" + "      ]\n" + "    }\n" + "  ]\n" + "}";
  }
}
