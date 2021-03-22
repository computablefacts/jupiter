package com.computablefacts.jupiter.storage.datastore;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.termstore.FieldCard;
import com.computablefacts.jupiter.storage.termstore.FieldCount;
import com.computablefacts.jupiter.storage.termstore.FieldLabels;
import com.computablefacts.jupiter.storage.termstore.IngestStats;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.computablefacts.jupiter.storage.termstore.TermCard;
import com.computablefacts.jupiter.storage.termstore.TermCount;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class DataStoreTest extends MiniAccumuloClusterTest {

  private static String normalize(String str) {
    String token = Codecs.nopTokenizer.apply(str).span(0).text();
    return Codecs.nopLexicoder.apply(token).text();
  }

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
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here

      Assert.assertEquals(10, list.size());
    }
  }

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
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here

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

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));

    Assert.assertTrue(dataStore.remove("first_dataset"));
    Assert.assertTrue(dataStore.remove("second_dataset"));

    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
  }

  @Test
  public void testRemoveRows() throws Exception {

    Authorizations auths = Constants.AUTH_ADM;
    DataStore dataStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));

    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) { // remove even rows from dataset 1
        Assert.assertTrue(dataStore.remove("first_dataset", Sets.newHashSet("row_" + i)));
      } else { // remove odd rows from dataset 2
        Assert.assertTrue(dataStore.remove("second_dataset", Sets.newHashSet("row_" + i)));
      }
    }

    Assert.assertEquals(5, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(5, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));

    // Ensure odd rows remain in dataset 1
    List<Blob<Value>> iterator1 = entitiesInFirstDataset(dataStore, auths);

    for (int i = 0; i < 5; i++) {
      Blob<Value> blob = iterator1.get(i);
      Assert.assertEquals("row_" + (2 * i + 1), blob.key());
    }

    // Ensure even rows remain in dataset 2
    List<Blob<Value>> iterator2 = entitiesInFirstDataset(dataStore, auths);

    for (int i = 0; i < 5; i++) {
      Blob<Value> blob = iterator2.get(i);
      Assert.assertEquals("row_" + (2 * i + 1), blob.key());
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
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    DataStore dataStore = newDataStore(auths);

    // BlobStore
    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));

    // Test TermStore's metadata
    FieldCard fca1 = fieldCardInFirstDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldCard fca2 = fieldCardInSecondDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldCard fca3 = fieldCardInThirdDataset(dataStore, "Actors[0]¤children[0]", auths);

    Assert.assertEquals("Actors[0]¤children[0]", fca1.field());
    Assert.assertEquals(Sets.newHashSet("FIRST_DATASET_CARD", "ADM"), fca1.labels());
    Assert.assertEquals(10, fca1.cardinality());

    Assert.assertEquals("Actors[0]¤children[0]", fca2.field());
    Assert.assertEquals(Sets.newHashSet("SECOND_DATASET_CARD", "ADM"), fca2.labels());
    Assert.assertEquals(10, fca2.cardinality());

    Assert.assertEquals("Actors[0]¤children[0]", fca3.field());
    Assert.assertEquals(Sets.newHashSet("THIRD_DATASET_CARD", "ADM"), fca3.labels());
    Assert.assertEquals(10, fca3.cardinality());

    FieldCount fco1 = fieldCountInFirstDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldCount fco2 = fieldCountInSecondDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldCount fco3 = fieldCountInThirdDataset(dataStore, "Actors[0]¤children[0]", auths);

    Assert.assertEquals("Actors[0]¤children[0]", fco1.field());
    Assert.assertEquals(Sets.newHashSet("FIRST_DATASET_CNT", "ADM"), fco1.labels());
    Assert.assertEquals(10, fco1.count());

    Assert.assertEquals("Actors[0]¤children[0]", fco2.field());
    Assert.assertEquals(Sets.newHashSet("SECOND_DATASET_CNT", "ADM"), fco2.labels());
    Assert.assertEquals(10, fco2.count());

    Assert.assertEquals("Actors[0]¤children[0]", fco3.field());
    Assert.assertEquals(Sets.newHashSet("THIRD_DATASET_CNT", "ADM"), fco3.labels());
    Assert.assertEquals(10, fco3.count());

    FieldLabels fl1 = fieldLabelsInFirstDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldLabels fl2 = fieldLabelsInSecondDataset(dataStore, "Actors[0]¤children[0]", auths);
    FieldLabels fl3 = fieldLabelsInThirdDataset(dataStore, "Actors[0]¤children[0]", auths);

    Assert.assertEquals("Actors[0]¤children[0]", fl1.field());
    Assert.assertEquals(Sets.newHashSet("FIRST_DATASET_VIZ", "ADM"), fl1.accumuloLabels());
    Assert.assertEquals(12, fl1.termLabels().size());

    Assert.assertEquals("Actors[0]¤children[0]", fl2.field());
    Assert.assertEquals(Sets.newHashSet("SECOND_DATASET_VIZ", "ADM"), fl2.accumuloLabels());
    Assert.assertEquals(12, fl2.termLabels().size());

    Assert.assertEquals("Actors[0]¤children[0]", fl3.field());
    Assert.assertEquals(Sets.newHashSet("THIRD_DATASET_VIZ", "ADM"), fl3.accumuloLabels());
    Assert.assertEquals(12, fl3.termLabels().size());

    // Test TermStore's terms
    Assert.assertEquals(1, countEntitiesInFirstDataset(dataStore, "suri",
        Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(1, countEntitiesInSecondDataset(dataStore, "suri",
        Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(1, countEntitiesInThirdDataset(dataStore, "suri",
        Sets.newHashSet("Actors[0]¤children[0]"), auths));

    Assert.assertTrue(dataStore.truncate());

    // BlobStore
    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInThirdDataset(dataStore, auths));

    // Test TermStore's metadata
    Assert.assertNull(fieldCardInFirstDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCardInSecondDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCardInThirdDataset(dataStore, "Actors[0]¤children[0]", auths));

    Assert.assertNull(fieldCountInFirstDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCountInSecondDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCountInThirdDataset(dataStore, "Actors[0]¤children[0]", auths));

    Assert.assertNull(fieldLabelsInFirstDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldLabelsInSecondDataset(dataStore, "Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldLabelsInThirdDataset(dataStore, "Actors[0]¤children[0]", auths));

    // Test TermStore's terms
    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, normalize("Suri"),
        Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, normalize("Suri"),
        Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(0, countEntitiesInThirdDataset(dataStore, normalize("Suri"),
        Sets.newHashSet("Actors[0]¤children[0]"), auths));
  }

  @Test
  public void testBlobScan() throws Exception {

    Authorizations auths1 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    Authorizations auths2 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4");
    DataStore dataStore = newDataStore(auths1);

    // Use the Scanner auths to filter data
    Set<Blob<Value>> list1 = new HashSet<>();

    try (Scanners scanners = dataStore.scanners(auths2)) { // keep order

      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, "first_dataset");

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list1.add(blob);
        Assert.assertEquals("row_" + (list1.size() - 1), blob.key());
      }
    }

    // Use the doc ids to filter data
    Set<Blob<Value>> list2 = new HashSet<>();

    try (
        Scanners scanners = new Scanners(dataStore.configurations(), dataStore.name(), auths1, 2)) { // out-of-order

      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, "first_dataset",
          Sets.newHashSet("row_0", "row_1", "row_2", "row_3", "row_4"));

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list2.add(blob);
      }
    }

    Assert.assertEquals(list1, list2);
  }

  @Test
  public void testSearchTerm() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        List<String> list = new ArrayList<>();
        dataStore.searchByTerm(scanners, writers, "first_dataset", normalize("Connor"), null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();
        dataStore.searchByTerm(scanners, writers, "first_dataset", normalize("Isabella Jane"), null,
            null).forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }
      }
    }
  }

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
            .searchByTerms(scanners, writers, "first_dataset",
                Sets.newHashSet(normalize("Connor"), normalize("Isabella Jane")), null)
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
        try (IngestStats stats = dataStore.newIngestStats()) {
          for (int i = 0; i < 10; i++) {
            Assert.assertTrue(dataStore.persist(writers, stats, "fourth_dataset", "row_" + i,
                json2(i), null, Codecs.defaultTokenizer));
          }
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
            "Actors[*]¤children[*]:\"Suri\" AND Actors[0]¤uuid:\"item?0\" AND Actors[1]¤uuid:\"item5?\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_5", list.get(0));

        list.clear();

        // Test an OR query
        QueryBuilder.build(
            "Actors[*]¤children[*]:\"Roel\" AND (Actors[0]¤uuid:\"item4?\" OR Actors[0]¤uuid:\"item5?\")")
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

        String cacheId = dataStore.writeCache(writers, set.iterator());

        Set<String> setNew = new HashSet<>();
        dataStore.readCache(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        dataStore.readCache(scanners, cacheId, "5").forEachRemaining(setNew::add);

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

        String cacheId = dataStore.writeCache(writers, set.iterator(), 5);

        Thread.sleep(1000);

        Set<String> setNew = new HashSet<>();
        dataStore.readCache(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        dataStore.readCache(scanners, cacheId, "5").forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("5", "6", "7", "8", "9"), setNew);
      }
    }
  }

  @Test
  public void testTermCount() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      dataStore.termCount(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      @Var
      TermCount tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.count());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Single dataset, hits backward index
      list.clear();
      dataStore.termCount(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.count());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Cross datasets
      list.clear();
      dataStore.termCount(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(3, list.get(0).getSecond().size());

      for (int i = 0; i < 3; i++) {

        tc = list.get(0).getSecond().get(i);

        Assert.assertEquals("Actors[0]¤children[2]", tc.field());
        Assert.assertEquals("connor", tc.term());
        Assert.assertEquals(10, tc.count());

        if (i == 0) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
              tc.labels());
        } else if (i == 1) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_"),
              tc.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_"),
              tc.labels());
        }
      }
    }
  }

  @Test
  public void testTermCard() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      dataStore.termCard(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      @Var
      TermCard tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Single dataset, hits backward index
      list.clear();
      dataStore.termCard(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Cross datasets
      list.clear();
      dataStore.termCard(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(3, list.get(0).getSecond().size());

      for (int i = 0; i < 3; i++) {

        tc = list.get(0).getSecond().get(i);

        Assert.assertEquals("Actors[0]¤children[2]", tc.field());
        Assert.assertEquals("connor", tc.term());
        Assert.assertEquals(10, tc.cardinality());

        if (i == 0) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
              tc.labels());
        } else if (i == 1) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_"),
              tc.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_"),
              tc.labels());
        }
      }
    }
  }

  @Test
  public void testTermScan() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<Term>>> list = new ArrayList<>();
      dataStore.termScan(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(10, list.get(0).getSecond().size());

      for (int i = 0; i < 10; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());
        Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
            "FIRST_DATASET_ROW_" + i), term.labels());
      }

      // Single dataset, hits backward index
      list.clear();
      dataStore.termScan(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(10, list.get(0).getSecond().size());

      for (int i = 0; i < 10; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());
        Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
            "FIRST_DATASET_ROW_" + i), term.labels());
      }

      // Cross datasets
      list.clear();
      dataStore.termScan(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(30, list.get(0).getSecond().size());

      for (int i = 0; i < 30; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + (i % 10), term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());

        if (i < 10) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
              "FIRST_DATASET_ROW_" + (i % 10)), term.labels());
        } else if (i < 20) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_",
              "SECOND_DATASET_ROW_" + (i % 10)), term.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_",
              "THIRD_DATASET_ROW_" + (i % 10)), term.labels());
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNumericalRangeScanOpenedBeginOpenedEnd() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
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

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", "55", "57",
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).docId());
      }
    }
  }

  @Test
  public void testNumericalRangeScanClosedBeginOpenedEnd() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", "56", null,
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).docId());
      }
    }
  }

  @Test
  public void testNumericalRangeScanOpenedBeginClosedEnd() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    DataStore dataStore = newDataStore(auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Term> list = new ArrayList<>();
      dataStore.numericalRangeScan(scanners, "first_dataset", null, "57",
          Sets.newHashSet("Actors[*]¤age"), null).forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("row_" + i, list.get(i).docId());
      }
    }
  }

  @Test
  public void testPersistBlobs() throws Exception {

    Authorizations auths = new Authorizations("BLOB_DATASET_RAW_DATA", "BLOB_DATASET_CNT",
        "BLOB_DATASET_CARD", "BLOB_DATASET_VIZ");
    DataStore dataStore = newDataStore(auths);
    Base64.Encoder b64Encoder = Base64.getEncoder();

    try (Writers writers = dataStore.writers()) {
      try (IngestStats stats = dataStore.newIngestStats()) {
        for (int i = 0; i < 10; i++) {
          String json = json1(i);
          String b64 = Codecs.encodeB64(b64Encoder, json);
          Assert.assertTrue(dataStore.persistBlob(writers, stats, "blob_dataset", "row_" + i, b64));
        }
      }
    }

    Base64.Decoder b64Decoder = Base64.getDecoder();

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      List<Blob<Value>> list = new ArrayList<>();
      dataStore.blobScan(scanners, "blob_dataset").forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {
        String b64 = list.get(i).value().toString();
        String json = Codecs.decodeB64(b64Decoder, b64);
        Assert.assertFalse(Codecs.asObject(json).isEmpty());
      }

      List<FieldCount> counts = new ArrayList<>();
      dataStore.fieldCount(scanners, "blob_dataset", "").forEachRemaining(counts::add);

      Assert.assertEquals(1, counts.size());
      Assert.assertEquals("", counts.get(0).field());
      Assert.assertEquals(2, counts.get(0).labels().size());
      Assert.assertEquals(10, counts.get(0).count());

      List<FieldCard> cards = new ArrayList<>();
      dataStore.fieldCard(scanners, "blob_dataset", "").forEachRemaining(cards::add);

      Assert.assertEquals(1, cards.size());
      Assert.assertEquals("", cards.get(0).field());
      Assert.assertEquals(2, cards.get(0).labels().size());
      Assert.assertEquals(10, cards.get(0).cardinality());

      List<FieldLabels> vizs = new ArrayList<>();
      dataStore.fieldLabels(scanners, "blob_dataset", "").forEachRemaining(vizs::add);

      Assert.assertEquals(1, vizs.size());
      Assert.assertEquals("", vizs.get(0).field());
      Assert.assertEquals(2, vizs.get(0).accumuloLabels().size());
      Assert.assertEquals(2, vizs.get(0).termLabels().size());
    }
  }

  private FieldLabels fieldLabelsInFirstDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldLabels(dataStore, "first_dataset", field, authorizations);
  }

  private FieldLabels fieldLabelsInSecondDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldLabels(dataStore, "second_dataset", field, authorizations);
  }

  private FieldLabels fieldLabelsInThirdDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldLabels(dataStore, "third_dataset", field, authorizations);
  }

  private FieldLabels fieldLabels(DataStore dataStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldLabels> iterator = dataStore.fieldLabels(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private FieldCount fieldCountInFirstDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCount(dataStore, "first_dataset", field, authorizations);
  }

  private FieldCount fieldCountInSecondDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCount(dataStore, "second_dataset", field, authorizations);
  }

  private FieldCount fieldCountInThirdDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCount(dataStore, "third_dataset", field, authorizations);
  }

  private FieldCount fieldCount(DataStore dataStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldCount> iterator = dataStore.fieldCount(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private FieldCard fieldCardInFirstDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCard(dataStore, "first_dataset", field, authorizations);
  }

  private FieldCard fieldCardInSecondDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCard(dataStore, "second_dataset", field, authorizations);
  }

  private FieldCard fieldCardInThirdDataset(DataStore dataStore, String field,
      Authorizations authorizations) {
    return fieldCard(dataStore, "third_dataset", field, authorizations);
  }

  private FieldCard fieldCard(DataStore dataStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldCard> iterator = dataStore.fieldCard(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private int countEntitiesInFirstDataset(DataStore dataStore, String term, Set<String> fields,
      Authorizations authorizations) {
    return count(dataStore, "first_dataset", term, fields, authorizations);
  }

  private int countEntitiesInSecondDataset(DataStore dataStore, String term, Set<String> fields,
      Authorizations authorizations) {
    return count(dataStore, "second_dataset", term, fields, authorizations);
  }

  private int countEntitiesInThirdDataset(DataStore dataStore, String term, Set<String> fields,
      Authorizations authorizations) {
    return count(dataStore, "third_dataset", term, fields, authorizations);
  }

  private int count(DataStore dataStore, String dataset, String term, Set<String> fields,
      Authorizations authorizations) {
    return entities(dataStore, dataset, term, fields, authorizations).size();
  }

  private int countEntitiesInFirstDataset(DataStore dataStore, Authorizations authorizations) {
    return count(dataStore, "first_dataset", authorizations);
  }

  private int countEntitiesInSecondDataset(DataStore dataStore, Authorizations authorizations) {
    return count(dataStore, "second_dataset", authorizations);
  }

  private int countEntitiesInThirdDataset(DataStore dataStore, Authorizations authorizations) {
    return count(dataStore, "third_dataset", authorizations);
  }

  private int count(DataStore dataStore, String dataset, Authorizations authorizations) {
    return entities(dataStore, dataset, authorizations).size();
  }

  private List<Pair<String, List<Term>>> entitiesInFirstDataset(DataStore dataStore, String term,
      Set<String> fields, Authorizations authorizations) {
    return entities(dataStore, "first_dataset", term, fields, authorizations);
  }

  private List<Pair<String, List<Term>>> entitiesInSecondDataset(DataStore dataStore, String term,
      Set<String> fields, Authorizations authorizations) {
    return entities(dataStore, "second_dataset", term, fields, authorizations);
  }

  private List<Pair<String, List<Term>>> entitiesInThirdDataset(DataStore dataStore, String term,
      Set<String> fields, Authorizations authorizations) {
    return entities(dataStore, "third_dataset", term, fields, authorizations);
  }

  private List<Pair<String, List<Term>>> entities(DataStore dataStore, String dataset, String term,
      Set<String> fields, Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) { // keep order

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      Iterator<Pair<String, List<Term>>> iterator =
          dataStore.termScan(scanners, dataset, term, fields, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private List<Blob<Value>> entitiesInFirstDataset(DataStore dataStore,
      Authorizations authorizations) {
    return entities(dataStore, "first_dataset", authorizations);
  }

  private List<Blob<Value>> entitiesInSecondDataset(DataStore dataStore,
      Authorizations authorizations) {
    return entities(dataStore, "second_dataset", authorizations);
  }

  private List<Blob<Value>> entitiesInThirdDataset(DataStore dataStore,
      Authorizations authorizations) {
    return entities(dataStore, "third_dataset", authorizations);
  }

  private List<Blob<Value>> entities(DataStore dataStore, String dataset,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) { // keep order

      List<Blob<Value>> list = new ArrayList<>();
      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, dataset);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private void fillDataStore(DataStore dataStore) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    try (Writers writers = dataStore.writers()) {
      try (IngestStats stats = dataStore.newIngestStats()) {

        for (int i = 0; i < 10; i++) {
          Assert.assertTrue(dataStore.persist(writers, stats, "first_dataset", "row_" + i, json1(i),
              key -> true, Codecs.nopTokenizer, Codecs.defaultLexicoder));
        }

        for (int i = 0; i < 10; i++) {
          Assert.assertTrue(dataStore.persist(writers, stats, "second_dataset", "row_" + i,
              json1(i), key -> true, Codecs.nopTokenizer, Codecs.defaultLexicoder));
        }

        for (int i = 0; i < 10; i++) {
          Assert.assertTrue(dataStore.persist(writers, stats, "third_dataset", "row_" + i, json1(i),
              key -> true, Codecs.nopTokenizer, Codecs.defaultLexicoder));
        }
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
