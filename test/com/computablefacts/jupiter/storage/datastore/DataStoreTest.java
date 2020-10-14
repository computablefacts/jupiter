package com.computablefacts.jupiter.storage.datastore;

import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;

public class DataStoreTest {

  @Test
  public void testCreateThenDestroyDataStore() throws IOException, InterruptedException {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.create();
    Configurations configurations =
        new Configurations(accumulo.getInstanceName(), accumulo.getZooKeepers(),
            MiniAccumuloClusterUtils.MAC_USER, MiniAccumuloClusterUtils.MAC_PASSWORD);
    DataStore dataStore = new DataStore(configurations, "store");

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());
    Assert.assertTrue(Tables.exists(configurations.tableOperations(), "storeBlobs"));
    Assert.assertTrue(Tables.all(configurations.tableOperations()).contains("storeBlobs"));
    Assert.assertTrue(Tables.exists(configurations.tableOperations(), "storeTerms"));
    Assert.assertTrue(Tables.all(configurations.tableOperations()).contains("storeTerms"));

    Assert.assertTrue(dataStore.destroy());
    Assert.assertFalse(dataStore.isReady());
    Assert.assertFalse(Tables.exists(configurations.tableOperations(), "storeBlobs"));
    Assert.assertFalse(Tables.all(configurations.tableOperations()).contains("storeBlobs"));
    Assert.assertFalse(Tables.exists(configurations.tableOperations(), "storeTerms"));
    Assert.assertFalse(Tables.all(configurations.tableOperations()).contains("storeTerms"));
  }
}
