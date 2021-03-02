package com.computablefacts.jupiter.storage;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class AbstractStorageTest extends MiniAccumuloClusterTest {

  @Test(expected = NullPointerException.class)
  public void testEncodeNull() {
    String str = AbstractStorage.encode(null);
  }

  @Test
  public void testEncode() {
    Assert.assertEquals("My\\u0000message\\u0000!", AbstractStorage.encode("My\0message\0!"));
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNull() {
    String str = AbstractStorage.decode(null);
  }

  @Test
  public void testDecode() {
    Assert.assertEquals("My\0message\0!", AbstractStorage.decode("My\\u0000message\\u0000!"));
  }

  @Test(expected = NullPointerException.class)
  public void testToVisibilityLabelNull() {
    String str = AbstractStorage.toVisibilityLabel(null);
  }

  @Test
  public void testToVisibilityLabel() {
    Assert.assertEquals("2001_0DB8_0001_0000_0000_0AB9_C0A8_0102",
        AbstractStorage.toVisibilityLabel("2001:0db8:0001:0000:0000:0ab9:C0A8:0102"));
    Assert.assertEquals("000_0000_00_00T00_00_00_000Z",
        AbstractStorage.toVisibilityLabel("000|0000-00-00T00:00:00.000Z"));
  }

  @Test
  public void testNullToEmpty() {
    Assert.assertEquals(Authorizations.EMPTY, AbstractStorage.nullToEmpty(null));
    Assert.assertEquals(Constants.AUTH_ADM, AbstractStorage.nullToEmpty(Constants.AUTH_ADM));
  }

  @Test
  public void testSetRange() {
    // TODO
  }

  @Test
  public void testSetRanges() {
    // TODO
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    AbstractStorage dataStore = new SimpleStorage(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());

    Assert.assertTrue(dataStore.create()); // ensure create is reentrant
    Assert.assertTrue(dataStore.isReady());
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    AbstractStorage dataStore = new SimpleStorage(configurations, tableName);

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());

    Assert.assertTrue(dataStore.destroy());
    Assert.assertFalse(dataStore.isReady());

    Assert.assertTrue(dataStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(dataStore.isReady());
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    AbstractStorage dataStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, auths));

    Assert.assertTrue(dataStore.truncate());

    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInFourthDataset(dataStore, auths));
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    AbstractStorage dataStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, auths));

    try (BatchDeleter deleter = dataStore.deleter(auths)) {
      Assert.assertTrue(dataStore.remove(deleter, Sets.newHashSet("first_dataset")));
      Assert.assertTrue(dataStore.remove(deleter, Sets.newHashSet("second_dataset")));
    }

    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, auths));
  }

  @Test
  public void testRemoveRows() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    AbstractStorage dataStore = newDataStore(auths);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, auths));

    try (BatchDeleter deleter = dataStore.deleter(auths)) {
      for (int i = 0; i < 100; i++) {
        if (i % 2 == 0) { // remove even rows from dataset 1
          Assert.assertTrue(dataStore.remove(deleter, "row_" + i, "first_dataset", null));
        } else { // remove odd rows from dataset 2
          Assert.assertTrue(dataStore.remove(deleter, "row_" + i, "second_dataset", null));
        }
      }
    }

    Assert.assertEquals(5, countEntitiesInFirstDataset(dataStore, auths));
    Assert.assertEquals(5, countEntitiesInSecondDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, auths));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, auths));

    // Ensure odd rows remain in dataset 1
    Iterator<Map.Entry<Key, Value>> iterator1 = entitiesInFirstDataset(dataStore, auths);

    while (iterator1.hasNext()) {
      Assert
          .assertEquals(1,
              Integer.parseInt(
                  iterator1.next().getKey().getRow().toString().substring("row_".length()), 10)
                  % 2);
    }

    // Ensure even rows remain in dataset 2
    Iterator<Map.Entry<Key, Value>> iterator2 = entitiesInSecondDataset(dataStore, auths);

    while (iterator2.hasNext()) {
      Assert
          .assertEquals(0,
              Integer.parseInt(
                  iterator2.next().getKey().getRow().toString().substring("row_".length()), 10)
                  % 2);
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    AbstractStorage dataStore = newDataStore(authsDS1);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, authsDS1));
    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, authsDS2)); // Throws an
                                                                               // exception
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    AbstractStorage dataStore = newDataStore(authsDS1DS2);

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, authsDS1DS2));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, authsDS1DS2));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, authsDS1DS2));
    Assert.assertEquals(10, countEntitiesInFourthDataset(dataStore, authsDS1DS2));

    Assert.assertEquals(10, countEntitiesInFirstDataset(dataStore, authsDS1));
    Assert.assertEquals(0, countEntitiesInSecondDataset(dataStore, authsDS1));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, authsDS1));
    Assert.assertEquals(0, countEntitiesInFourthDataset(dataStore, authsDS1));

    Assert.assertEquals(0, countEntitiesInFirstDataset(dataStore, authsDS2));
    Assert.assertEquals(10, countEntitiesInSecondDataset(dataStore, authsDS2));
    Assert.assertEquals(10, countEntitiesInThirdDataset(dataStore, authsDS2));
    Assert.assertEquals(0, countEntitiesInFourthDataset(dataStore, authsDS2));
  }

  @Test
  public void testScanners() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    AbstractStorage dataStore = newDataStore(authsDS1);

    try (Scanner scanner = dataStore.scanner(authsDS1)) { // keep order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }

    try (BatchScanner scanner = dataStore.batchScanner(authsDS1)) { // out of order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }
  }

  private int countEntitiesInFirstDataset(AbstractStorage store, Authorizations authorizations) {
    return countEntities(store, "first_dataset", authorizations);
  }

  private int countEntitiesInSecondDataset(AbstractStorage store, Authorizations authorizations) {
    return countEntities(store, "second_dataset", authorizations);
  }

  private int countEntitiesInThirdDataset(AbstractStorage store, Authorizations authorizations) {
    return countEntities(store, "third_dataset", authorizations);
  }

  private int countEntitiesInFourthDataset(AbstractStorage store, Authorizations authorizations) {
    return countEntities(store, "fourth_dataset", authorizations);
  }

  private int countEntities(AbstractStorage store, String dataset, Authorizations authorizations) {
    Iterator<Map.Entry<Key, Value>> iterator = entities(store, dataset, authorizations);
    return iterator.hasNext() ? Iterators.size(iterator) : 0;
  }

  private Iterator<Map.Entry<Key, Value>> entitiesInFirstDataset(AbstractStorage store,
      Authorizations authorizations) {
    return entities(store, "first_dataset", authorizations);
  }

  private Iterator<Map.Entry<Key, Value>> entitiesInSecondDataset(AbstractStorage store,
      Authorizations authorizations) {
    return entities(store, "second_dataset", authorizations);
  }

  private Iterator<Map.Entry<Key, Value>> entitiesInThirdDataset(AbstractStorage store,
      Authorizations authorizations) {
    return entities(store, "third_dataset", authorizations);
  }

  private Iterator<Map.Entry<Key, Value>> entitiesInFourthDataset(AbstractStorage store,
      Authorizations authorizations) {
    return entities(store, "fourth_dataset", authorizations);
  }

  private Iterator<Map.Entry<Key, Value>> entities(AbstractStorage store, String dataset,
      Authorizations authorizations) {

    Preconditions.checkNotNull(store, "storage should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanner scanner = store.scanner(authorizations)) { // keep order
      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text(dataset));
      return scanner.iterator();
    }
  }

  private void fillDataStore(AbstractStorage store) throws Exception {

    Preconditions.checkNotNull(store, "storage should not be null");

    try (BatchWriter writer = store.writer()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(store.add(writer, new Text("row_" + i), new Text("first_dataset"), null,
            new ColumnVisibility("DS_1"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(store.add(writer, new Text("row_" + i), new Text("second_dataset"), null,
            new ColumnVisibility("DS_2"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(store.add(writer, new Text("row_" + i), new Text("third_dataset"), null,
            new ColumnVisibility("DS_1|DS_2"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(store.add(writer, new Text("row_" + i), new Text("fourth_dataset"), null,
            new ColumnVisibility("DS_1&DS_2"), new Value()));
      }
    }
  }

  private AbstractStorage newDataStore(Authorizations auths) throws Exception {

    String tableName = nextTableName();
    String username = nextUsername();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    AbstractStorage dataStore = new SimpleStorage(configurations, tableName);

    if (dataStore.create()) {
      fillDataStore(dataStore);
    }

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return dataStore;
  }

  private static class SimpleStorage extends AbstractStorage {

    public SimpleStorage(Configurations configurations, String tableName) {
      super(configurations, tableName);
    }
  }
}
