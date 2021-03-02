package com.computablefacts.jupiter.storage.blobstore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class BlobStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testAddLocalityGroup() throws Exception {

    BlobStore blobStore = newDataStore(Constants.AUTH_ADM);

    Assert.assertTrue(
            Tables.getLocalityGroups(blobStore.configurations().tableOperations(), blobStore.tableName()).isEmpty());

    Assert.assertTrue(blobStore.addLocalityGroup("third_dataset"));
    Assert.assertFalse(
            Tables.getLocalityGroups(blobStore.configurations().tableOperations(), blobStore.tableName()).isEmpty());

    Assert.assertTrue(blobStore.addLocalityGroup("third_dataset")); // ensure reentrant
    Assert.assertFalse(
            Tables.getLocalityGroups(blobStore.configurations().tableOperations(), blobStore.tableName()).isEmpty());
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    BlobStore blobStore = new BlobStore(configurations, tableName);

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    Assert.assertTrue(blobStore.create()); // ensure create is reentrant
    Assert.assertTrue(blobStore.isReady());
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    BlobStore blobStore = new BlobStore(configurations, tableName);

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    Assert.assertTrue(blobStore.destroy());
    Assert.assertFalse(blobStore.isReady());

    Assert.assertTrue(blobStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(blobStore.isReady());
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    BlobStore blobStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(blobStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore, auths));

    Assert.assertTrue(blobStore.truncate());

    Assert.assertEquals(0, countEntitiesInFirstDataset(blobStore, auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(blobStore, auths));
    Assert.assertEquals(0, countEntitiesInThirdDataset(blobStore, auths));
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    BlobStore blobStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,auths));

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeDataset(deleter, "first_dataset"));
      Assert.assertTrue(blobStore.removeDataset(deleter, "second_dataset"));
    }

    Assert.assertEquals(0, countEntitiesInFirstDataset(blobStore,auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,auths));
  }

  @Test
  public void testRemoveBlobs() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    BlobStore blobStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,auths));

    Set<String> odd = new HashSet<>();
    Set<String> even = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) { // remove even rows from dataset 1
        even.add("row_" + i);
      } else { // remove odd rows from dataset 2
        odd.add("row_" + i);
      }
    }

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeKeys(deleter, "first_dataset", even));
      Assert.assertTrue(blobStore.removeKeys(deleter, "second_dataset", odd));
    }

    Assert.assertEquals(5, countEntitiesInFirstDataset(blobStore,auths));
    Assert.assertEquals(5, countEntitiesInSecondDataset(blobStore,auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,auths));

    // Ensure odd rows remain in dataset 1
    List<Blob<Value>> list1 = entitiesInThirstDataset(blobStore,auths);

    for (Blob<Value> blob : list1) {
      Assert.assertEquals(1, Integer.parseInt(blob.key().substring("row_".length()), 10) % 2);
    }

    // Ensure even rows remain in dataset 2
    List<Blob<Value>> list2 = entitiesInSecondDataset(blobStore,auths);

    for (Blob<Value> blob : list2) {
      Assert.assertEquals(0, Integer.parseInt(blob.key().substring("row_".length()), 10) % 2);
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    BlobStore blobStore = newDataStore(authsDS1);

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,authsDS1));
    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,authsDS2)); // Throws an exception
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    BlobStore blobStore = newDataStore(authsDS1DS2);

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,authsDS1DS2));
    Assert.assertEquals(10, countEntitiesInSecondDataset(blobStore,authsDS1DS2));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,authsDS1DS2));

    Assert.assertEquals(10, countEntitiesInFirstDataset(blobStore,authsDS1));
    Assert.assertEquals(0, countEntitiesInSecondDataset(blobStore,authsDS1));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,authsDS1));

    Assert.assertEquals(0, countEntitiesInFirstDataset(blobStore,authsDS2));
    Assert.assertEquals(10, countEntitiesInSecondDataset(blobStore,authsDS2));
    Assert.assertEquals(10, countEntitiesInThirdDataset(blobStore,authsDS2));
  }

  @Test
  public void testScanners() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    BlobStore blobStore = newDataStore(authsDS1);

    try (Scanner scanner = blobStore.scanner(authsDS1)) { // keep order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }

    try (BatchScanner scanner = blobStore.batchScanner(authsDS1)) { // out of order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }
  }

  @Test
  public void testGetOneBlob() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    BlobStore blobStore = newDataStore(auths);

    try (BatchScanner scanner = blobStore.batchScanner(auths)) { // out of order

      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset", "row_1");
      Blob<Value> blob = iterator.next();

      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("row_1", blob.key());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
      Assert.assertEquals(json(1), blob.value().toString());
    }
  }

  @Test
  public void testGetMoreThanOneBlob() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    BlobStore blobStore = newDataStore(auths);

    try (Scanner scanner = blobStore.scanner(auths)) { // keep order

      @Var
      int i = 0;
      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset");

      while (iterator.hasNext()) {

        Blob<Value> blob = iterator.next();

        Assert.assertEquals("row_" + i, blob.key());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
        Assert.assertEquals(json(i), blob.value().toString());

        i++;
      }
    }
  }

  private int countEntitiesInFirstDataset(BlobStore blobStore, Authorizations authorizations) {
    return count(blobStore, "first_dataset", authorizations);
  }

  private int countEntitiesInSecondDataset(BlobStore blobStore, Authorizations authorizations) {
    return count(blobStore, "second_dataset", authorizations);
  }

  private int countEntitiesInThirdDataset(BlobStore blobStore, Authorizations authorizations) {
    return count(blobStore, "third_dataset", authorizations);
  }

  private int count(BlobStore blobStore, String dataset, Authorizations authorizations) {
    return entities(blobStore, dataset, authorizations).size();
  }

  private List<Blob<Value>> entitiesInThirstDataset(BlobStore blobStore,
      Authorizations authorizations) {
    return entities(blobStore, "first_dataset", authorizations);
  }

  private List<Blob<Value>> entitiesInSecondDataset(BlobStore blobStore,
      Authorizations authorizations) {
    return entities(blobStore, "second_dataset", authorizations);
  }

  private List<Blob<Value>> entitiesInThirdDataset(BlobStore blobStore,
      Authorizations authorizations) {
    return entities(blobStore, "third_dataset", authorizations);
  }

  private List<Blob<Value>> entities(BlobStore blobStore, String dataset,
      Authorizations authorizations) {

    Preconditions.checkNotNull(blobStore, "blobStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanner scanner = blobStore.scanner(authorizations)) { // keep order

      List<Blob<Value>> list = new ArrayList<>();
      Iterator<Blob<Value>> iterator = blobStore.get(scanner, dataset);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private void fillDataStore(BlobStore blobStore) throws Exception {

    Preconditions.checkNotNull(blobStore, "blobStore should not be null");

    try (BatchWriter writer = blobStore.writer()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            blobStore.put(writer, "first_dataset", "row_" + i, Sets.newHashSet("DS_1"), json(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            blobStore.put(writer, "second_dataset", "row_" + i, Sets.newHashSet("DS_2"), json(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(blobStore.put(writer, "third_dataset", "row_" + i,
            Sets.newHashSet("DS_1", "DS_2"), json(i)));
      }
    }
  }

  private BlobStore newDataStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newDataStore(auths, username);
  }

  private BlobStore newDataStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    BlobStore blobStore = new BlobStore(configurations, tableName);

    if (blobStore.create()) {
      fillDataStore(blobStore);
    }

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return blobStore;
  }

  private String json(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }]}";
  }
}
