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
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * This class is not thread-safe because {@link MiniAccumuloClusterUtils#setUserAuths} is used. Do
 * not execute methods in parallel.
 */
@net.jcip.annotations.NotThreadSafe
public class AbstractStorageTest {

  private static MiniAccumuloCluster accumulo;
  private static Configurations configurations;
  private static SimpleStorage storage;

  @BeforeClass
  public static void initClass() throws Exception {
    accumulo = MiniAccumuloClusterUtils.newCluster();
    configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    storage = new SimpleStorage(configurations, "storage");
  }

  @AfterClass
  public static void uinitClass() throws Exception {
    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private static void fill() throws Exception {

    Preconditions.checkNotNull(storage, "storage should not be null");

    try (BatchWriter writer = storage.writer()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(storage.add(writer, new Text("row_" + i), new Text("first_dataset"), null,
            new ColumnVisibility("DS_1"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(storage.add(writer, new Text("row_" + i), new Text("second_dataset"),
            null, new ColumnVisibility("DS_2"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(storage.add(writer, new Text("row_" + i), new Text("third_dataset"), null,
            new ColumnVisibility("DS_1|DS_2"), new Value()));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(storage.add(writer, new Text("row_" + i), new Text("fourth_dataset"),
            null, new ColumnVisibility("DS_1&DS_2"), new Value()));
      }
    }
  }

  private static int countFirst(Authorizations authorizations) {
    return count("first_dataset", authorizations);
  }

  private static int countSecond(Authorizations authorizations) {
    return count("second_dataset", authorizations);
  }

  private static int countThird(Authorizations authorizations) {
    return count("third_dataset", authorizations);
  }

  private static int countFourth(Authorizations authorizations) {
    return count("fourth_dataset", authorizations);
  }

  private static int count(String dataset, Authorizations authorizations) {
    Iterator<Map.Entry<Key, Value>> iterator = all(dataset, authorizations);
    return iterator.hasNext() ? Iterators.size(iterator) : 0;
  }

  private static Iterator<Map.Entry<Key, Value>> firstDataset(Authorizations authorizations) {
    return all("first_dataset", authorizations);
  }

  private static Iterator<Map.Entry<Key, Value>> secondDataset(Authorizations authorizations) {
    return all("second_dataset", authorizations);
  }

  private static Iterator<Map.Entry<Key, Value>> thirdDataset(Authorizations authorizations) {
    return all("third_dataset", authorizations);
  }

  private static Iterator<Map.Entry<Key, Value>> fourthDataset(Authorizations authorizations) {
    return all("fourth_dataset", authorizations);
  }

  private static Iterator<Map.Entry<Key, Value>> all(String dataset,
      Authorizations authorizations) {

    Preconditions.checkNotNull(storage, "storage should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanner scanner = storage.scanner(authorizations)) { // keep order
      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text(dataset));
      return scanner.iterator();
    }
  }

  @Before
  public void initMethods() throws Exception {
    if (storage.isReady()) {
      boolean isOk = storage.destroy();
    }
    if (storage.create()) {
      fill();
    }
  }

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
    Assert.assertEquals(new Authorizations("ADM"),
        AbstractStorage.nullToEmpty(new Authorizations("ADM")));
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

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_create");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    Assert.assertTrue(storage.create()); // ensure create is reentrant
    Assert.assertTrue(storage.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_destroy");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    Assert.assertTrue(storage.destroy());
    Assert.assertFalse(storage.isReady());

    Assert.assertTrue(storage.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(storage.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
    Assert.assertEquals(10, countFourth(auths));

    Assert.assertTrue(storage.truncate());

    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(0, countThird(auths));
    Assert.assertEquals(0, countFourth(auths));
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
    Assert.assertEquals(10, countFourth(auths));

    try (BatchDeleter deleter = storage.deleter(auths)) {
      Assert.assertTrue(storage.remove(deleter, Sets.newHashSet("first_dataset")));
      Assert.assertTrue(storage.remove(deleter, Sets.newHashSet("second_dataset")));
    }

    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
    Assert.assertEquals(10, countFourth(auths));
  }

  @Test
  public void testRemoveRows() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
    Assert.assertEquals(10, countFourth(auths));

    try (BatchDeleter deleter = storage.deleter(auths)) {
      for (int i = 0; i < 100; i++) {
        if (i % 2 == 0) { // remove even rows from dataset 1
          Assert.assertTrue(storage.remove(deleter, "row_" + i, "first_dataset", null));
        } else { // remove odd rows from dataset 2
          Assert.assertTrue(storage.remove(deleter, "row_" + i, "second_dataset", null));
        }
      }
    }

    Assert.assertEquals(5, countFirst(auths));
    Assert.assertEquals(5, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
    Assert.assertEquals(10, countFourth(auths));

    // Ensure odd rows remain in dataset 1
    Iterator<Map.Entry<Key, Value>> iterator1 = firstDataset(auths);

    while (iterator1.hasNext()) {
      Assert
          .assertEquals(1,
              Integer.parseInt(
                  iterator1.next().getKey().getRow().toString().substring("row_".length()), 10)
                  % 2);
    }

    // Ensure even rows remain in dataset 2
    Iterator<Map.Entry<Key, Value>> iterator2 = secondDataset(auths);

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

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    Assert.assertEquals(10, countFirst(authsDS1));
    Assert.assertEquals(10, countFirst(authsDS2)); // Throws an exception
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1DS2);

    Assert.assertEquals(10, countFirst(authsDS1DS2));
    Assert.assertEquals(10, countSecond(authsDS1DS2));
    Assert.assertEquals(10, countThird(authsDS1DS2));
    Assert.assertEquals(10, countFourth(authsDS1DS2));

    Assert.assertEquals(10, countFirst(authsDS1));
    Assert.assertEquals(0, countSecond(authsDS1));
    Assert.assertEquals(10, countThird(authsDS1));
    Assert.assertEquals(0, countFourth(authsDS1));

    Assert.assertEquals(0, countFirst(authsDS2));
    Assert.assertEquals(10, countSecond(authsDS2));
    Assert.assertEquals(10, countThird(authsDS2));
    Assert.assertEquals(0, countFourth(authsDS2));
  }

  @Test
  public void testScanners() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    try (Scanner scanner = storage.scanner(authsDS1)) { // keep order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }

    try (BatchScanner scanner = storage.batchScanner(authsDS1)) { // out of order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }
  }

  private static class SimpleStorage extends AbstractStorage {

    public SimpleStorage(Configurations configurations, String tableName) {
      super(configurations, tableName);
    }
  }
}
