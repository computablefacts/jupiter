package com.computablefacts.jupiter.storage.blobstore;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;

public class BlobStoreTest {

  @Test
  public void testCreateThenDestroyBlobStore()
      throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());

    Assert.assertTrue(blobStore.destroy());
    Assert.assertFalse(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());
  }
}
