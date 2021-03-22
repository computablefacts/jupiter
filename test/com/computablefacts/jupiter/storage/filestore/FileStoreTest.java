package com.computablefacts.jupiter.storage.filestore;

import java.io.RandomAccessFile;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class FileStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testAddLocalityGroup() throws Exception {

    FileStore fileStore = newDataStore(Constants.AUTH_ADM);

    Assert.assertTrue(Tables
        .getLocalityGroups(fileStore.configurations().tableOperations(), fileStore.tableName())
        .isEmpty());

    Assert.assertTrue(fileStore.addLocalityGroup("1mb_dataset"));
    Assert.assertFalse(Tables
        .getLocalityGroups(fileStore.configurations().tableOperations(), fileStore.tableName())
        .isEmpty());

    Assert.assertTrue(fileStore.addLocalityGroup("10mb_dataset")); // ensure reentrant
    Assert.assertFalse(Tables
        .getLocalityGroups(fileStore.configurations().tableOperations(), fileStore.tableName())
        .isEmpty());
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    FileStore fileStore = new FileStore(configurations, tableName);

    Assert.assertTrue(fileStore.create());
    Assert.assertTrue(fileStore.isReady());

    Assert.assertTrue(fileStore.create()); // ensure create is reentrant
    Assert.assertTrue(fileStore.isReady());
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    FileStore fileStore = new FileStore(configurations, tableName);

    Assert.assertTrue(fileStore.create());
    Assert.assertTrue(fileStore.isReady());

    Assert.assertTrue(fileStore.destroy());
    Assert.assertFalse(fileStore.isReady());

    Assert.assertTrue(fileStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(fileStore.isReady());
  }

  @Test
  public void testTruncate() throws Exception {
    // TODO
  }

  @Test
  public void testRemoveDataset() throws Exception {
    // TODO
  }

  @Test
  public void testRemoveBlobs() throws Exception {
    // TODO
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {
    // TODO
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {
    // TODO
  }

  @Test
  public void testScanners() throws Exception {
    // TODO
  }

  @Test
  public void testGetOneBlob() throws Exception {
    // TODO
  }

  @Test
  public void testGetMoreThanOneBlob() throws Exception {
    // TODO
  }

  private void fillDataStore(FileStore fileStore) throws Exception {

    Preconditions.checkNotNull(fileStore, "fileStore should not be null");

    try (BatchWriter writer = fileStore.writer()) {

      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(fileStore.put(writer, "1mb_dataset", "row_" + i, Sets.newHashSet("DS_1"),
            createFile(1024)));
      }

      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(fileStore.put(writer, "10mb_dataset", "row_" + i,
            Sets.newHashSet("DS_10"), createFile(10 * 1024)));
      }

      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(fileStore.put(writer, "100mb_dataset", "row_" + i,
            Sets.newHashSet("DS_100"), createFile(100 * 1024)));
      }
    }
  }

  private java.io.File createFile(int size) throws Exception {
    java.io.File file = java.io.File.createTempFile("tmp-", ".txt");
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.setLength(size);
    }
    return file;
  }

  private FileStore newDataStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newDataStore(auths, username);
  }

  private FileStore newDataStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    FileStore fileStore = new FileStore(configurations, tableName);

    if (fileStore.create()) {
      fillDataStore(fileStore);
    }

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return fileStore;
  }
}
