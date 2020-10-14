package com.computablefacts.jupiter.storage.termstore;

import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;

public class TermStoreTest {

  @Test
  public void testCreateThenDestroyTermStore() throws IOException, InterruptedException {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.create();
    Configurations configurations =
        new Configurations(accumulo.getInstanceName(), accumulo.getZooKeepers(),
            MiniAccumuloClusterUtils.MAC_USER, MiniAccumuloClusterUtils.MAC_PASSWORD);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());
    Assert.assertTrue(Tables.exists(configurations.tableOperations(), "terms"));
    Assert.assertTrue(Tables.all(configurations.tableOperations()).contains("terms"));

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());
    Assert.assertFalse(Tables.exists(configurations.tableOperations(), "terms"));
    Assert.assertFalse(Tables.all(configurations.tableOperations()).contains("terms"));
  }
}
