package com.computablefacts.jupiter.storage.datastore;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;

public class DataStoreTest {

  @Test
  public void testCreateThenDestroyDataStore() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    DataStore dataStore = new DataStore(configurations, "store");

    Assert.assertTrue(dataStore.create());
    Assert.assertTrue(dataStore.isReady());

    Assert.assertTrue(dataStore.destroy());
    Assert.assertFalse(dataStore.isReady());
  }
}
