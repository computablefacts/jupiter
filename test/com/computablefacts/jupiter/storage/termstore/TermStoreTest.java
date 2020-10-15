package com.computablefacts.jupiter.storage.termstore;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;

public class TermStoreTest {

  @Test
  public void testCreateThenDestroyTermStore() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());
  }
}
