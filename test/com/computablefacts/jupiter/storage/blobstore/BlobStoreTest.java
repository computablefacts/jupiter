package com.computablefacts.jupiter.storage.blobstore;

import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;

public class BlobStoreTest {

  @Test
  public void testCreateThenDestroyBlobStore() throws IOException, InterruptedException {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.create();
    Configurations configurations =
        new Configurations(accumulo.getInstanceName(), accumulo.getZooKeepers(),
            MiniAccumuloClusterUtils.MAC_USER, MiniAccumuloClusterUtils.MAC_PASSWORD);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());
    Assert.assertTrue(Tables.exists(configurations.tableOperations(), "blobs"));
    Assert.assertTrue(Tables.all(configurations.tableOperations()).contains("blobs"));

    Assert.assertTrue(blobStore.destroy());
    Assert.assertFalse(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());
    Assert.assertFalse(Tables.exists(configurations.tableOperations(), "blobs"));
    Assert.assertFalse(Tables.all(configurations.tableOperations()).contains("blobs"));
  }
}
