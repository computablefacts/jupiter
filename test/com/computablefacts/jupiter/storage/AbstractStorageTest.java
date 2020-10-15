package com.computablefacts.jupiter.storage;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class AbstractStorageTest {

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
  public void testCreateFillAndTruncate() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_truncate");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    fill(storage);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(storage, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    Assert.assertTrue(storage.truncate());

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "third_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(0, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndRemoveDataset() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_remove");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    fill(storage);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(storage, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    try (BatchDeleter deleter = storage.deleter(new Authorizations("DS_1", "DS_2"))) {
      storage.remove(deleter, Sets.newHashSet("second_dataset"));
    }

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(storage, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(storage, "fourth_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(0, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndRemoveRow() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_remove");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    fill(storage);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(100, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    try (BatchDeleter deleter = storage.deleter(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 0; i < 100; i++) { // remove even rows in dataset 1
        if (i % 2 == 0) {
          Assert.assertTrue(storage.remove(deleter, "row_" + i, "first_dataset", null));
        } else { // remove odd rows in dataset 2
          Assert.assertTrue(storage.remove(deleter, "row_" + i, "second_dataset", null));
        }
      }
    }

    Assert.assertEquals(50, count(storage, "first_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(50, count(storage, "second_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "third_dataset", new Authorizations("DS_1", "DS_2")));
    Assert.assertEquals(100, count(storage, "fourth_dataset", new Authorizations("DS_1", "DS_2")));

    try (Scanner scanner = storage.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 1; i < 100; i += 2) { // ensure odd rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "first_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    try (Scanner scanner = storage.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 0; i < 100; i += 2) { // ensure even rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "second_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    try (Scanner scanner = storage.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 0; i < 100; i++) { // ensure all rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "third_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    try (Scanner scanner = storage.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 0; i < 100; i++) { // ensure all rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "fourth_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private void fill(AbstractStorage storage) throws Exception {

    Preconditions.checkNotNull(storage, "storage should not be null");

    try (BatchWriter writer = storage.writer()) {

      for (int i = 0; i < 100; i++) {
        boolean isOk = storage.add(writer, new Text("row_" + i), new Text("first_dataset"), null,
            new ColumnVisibility("DS_1"), new Value());
      }

      for (int i = 0; i < 100; i++) {
        boolean isOk = storage.add(writer, new Text("row_" + i), new Text("second_dataset"), null,
            new ColumnVisibility("DS_2"), new Value());
      }

      for (int i = 0; i < 100; i++) {
        boolean isOk = storage.add(writer, new Text("row_" + i), new Text("third_dataset"), null,
            new ColumnVisibility("DS_1|DS_2"), new Value());
      }

      for (int i = 0; i < 100; i++) {
        boolean isOk = storage.add(writer, new Text("row_" + i), new Text("fourth_dataset"), null,
            new ColumnVisibility("DS_1&DS_2"), new Value());
      }
    }
  }

  private int count(AbstractStorage storage, String dataset, Authorizations authorizations) {

    Preconditions.checkNotNull(storage, "storage should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (BatchScanner scanner = storage.batchScanner(authorizations)) {
      AbstractStorage.setRange(scanner, new Range());
      scanner.fetchColumnFamily(new Text(dataset));
      return Iterators.size(scanner.iterator());
    }
  }

  private static class SimpleStorage extends AbstractStorage {

    public SimpleStorage(Configurations configurations, String tableName) {
      super(configurations, tableName);
    }
  }
}
