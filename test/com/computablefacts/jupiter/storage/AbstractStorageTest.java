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

    Assert.assertEquals(1000, count(storage));
    Assert.assertTrue(storage.truncate());
    Assert.assertEquals(0, count(storage));

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndRemove() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    AbstractStorage storage = new SimpleStorage(configurations, "table_remove");

    Assert.assertTrue(storage.create());
    Assert.assertTrue(storage.isReady());

    fill(storage);

    Assert.assertEquals(1000, count(storage));

    try (BatchDeleter deleter = storage.deleter(Constants.AUTH_ADM)) {
      for (int rowId = 0; rowId < 10; rowId += 2) { // remove even rows
        Assert.assertTrue(AbstractStorage.setRange(deleter, Range.exact("row_" + rowId)));
        deleter.delete();
      }
    }

    Assert.assertEquals(500, count(storage));

    try (Scanner scanner = storage.scanner(Constants.AUTH_ADM)) {
      for (int rowId = 1; rowId < 10; rowId += 2) { // ensure odd rows exist
        Assert.assertTrue(AbstractStorage.setRange(scanner, Range.exact("row_" + rowId)));
        Assert.assertEquals(100, Iterators.size(scanner.iterator()));
      }
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private void fill(AbstractStorage storage) throws Exception {

    Preconditions.checkNotNull(storage, "storage should not be null");

    try (BatchWriter writer = storage.writer()) {
      for (int rowId = 0; rowId < 10; rowId++) {
        for (int cf = 0; cf < 10; cf++) {
          for (int cq = 0; cq < 10; cq++) {
            boolean isOk = storage.add(writer, new Text("row_" + rowId), new Text("cf_" + cf),
                new Text("cq_" + cq), new ColumnVisibility(Constants.STRING_ADM),
                new Value("value_" + rowId + "_" + cf + "_" + cq));
          }
        }
      }
    }
  }

  private int count(AbstractStorage storage) {

    Preconditions.checkNotNull(storage, "storage should not be null");

    try (BatchScanner scanner = storage.batchScanner(Constants.AUTH_ADM)) {
      boolean isOk = AbstractStorage.setRange(scanner, new Range());
      return Iterators.size(scanner.iterator());
    }
  }

  private static class SimpleStorage extends AbstractStorage {

    public SimpleStorage(Configurations configurations, String tableName) {
      super(configurations, tableName);
    }
  }
}
