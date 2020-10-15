package com.computablefacts.jupiter.storage.blobstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class BlobStoreTest {

  @Test
  public void testCreateThenDestroyBlobStore() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());

    Assert.assertTrue(blobStore.destroy());
    Assert.assertFalse(blobStore.isReady());
    Assert.assertEquals("blobs", blobStore.tableName());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndRemoveDataset() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    fill(blobStore);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1"));

    Assert.assertEquals(100, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_1")));

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_2"));

    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(100, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    try (BatchDeleter deleter = blobStore.deleter(new Authorizations("DS_1"))) {
      Assert.assertTrue(blobStore.removeDataset(deleter, "second_dataset")); // it is a no-op
    }

    Assert.assertEquals(100, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    try (BatchDeleter deleter = blobStore.deleter(new Authorizations("DS_2"))) {
      Assert.assertTrue(blobStore.removeDataset(deleter, "second_dataset")); // delete!
    }

    Assert.assertEquals(100, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_1")));

    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_2")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndRemoveKeys() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    fill(blobStore);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(100, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "second_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(100, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    try (BatchDeleter deleter = blobStore.deleter(new Authorizations("DS_1", "DS_2"))) {

      Set<String> even = new HashSet<>();
      Set<String> odd = new HashSet<>();

      for (int i = 0; i < 100; i++) {
        if (i % 2 == 0) {
          even.add("row_" + i);
        } else {
          odd.add("row_" + i);
        }
      }

      // remove even rows
      Assert.assertTrue(blobStore.removeKeys(deleter, "first_dataset", even));

      // remove odd rows
      Assert.assertTrue(blobStore.removeKeys(deleter, "second_dataset", odd));

      // remove all rows
      Assert.assertTrue(blobStore.removeKeys(deleter, "third_dataset", Sets.union(odd, even)));
    }

    Assert.assertEquals(50, count(blobStore, "first_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "first_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(0, count(blobStore, "second_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(50, count(blobStore, "second_dataset", new Authorizations("DS_2")));

    Assert.assertEquals(0, count(blobStore, "third_dataset", new Authorizations("DS_1")));
    Assert.assertEquals(0, count(blobStore, "third_dataset", new Authorizations("DS_2")));

    try (Scanner scanner = blobStore.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 1; i < 100; i += 2) { // ensure odd rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "first_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    try (Scanner scanner = blobStore.scanner(new Authorizations("DS_1", "DS_2"))) {
      for (int i = 0; i < 100; i += 2) { // ensure even rows exist
        Assert.assertTrue(
            AbstractStorage.setRange(scanner, Range.exact("row_" + i, "second_dataset")));
        Assert.assertEquals(1, Iterators.size(scanner.iterator()));
      }
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testGetASingleBlob() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    fill(blobStore);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    try (BatchScanner scanner = blobStore.batchScanner(new Authorizations("DS_1", "DS_2"))) {

      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset", "row_1");
      Blob<Value> blob = iterator.next();

      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("row_1", blob.key());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
      Assert.assertEquals(json(1), blob.value().toString());
    }
  }

  @Test
  public void testGetManyBlobs() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    fill(blobStore);

    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    try (Scanner scanner = blobStore.scanner(new Authorizations("DS_1", "DS_2"))) {

      // build the list of all keys and sort them in lexicographic order (the same order will be
      // used by the Accumulo Scanner to return the blobs)
      List<String> keys = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        keys.add("row_" + i);
      }

      Collections.sort(keys);

      // enumerate all blobs
      @Var
      int i = 0;
      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset");

      while (iterator.hasNext()) {

        Blob<Value> blob = iterator.next();

        Assert.assertEquals(keys.get(i), blob.key());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
        Assert.assertEquals(json(Integer.parseInt(keys.get(i).substring("row_".length()), 10)),
            blob.value().toString());

        i++;
      }
    }
  }

  private void fill(BlobStore blobStore) throws Exception {

    Preconditions.checkNotNull(blobStore, "blobStore should not be null");

    try (BatchWriter writer = blobStore.writer()) {

      for (int i = 0; i < 100; i++) {
        boolean isOk =
            blobStore.put(writer, "first_dataset", "row_" + i, Sets.newHashSet("DS_1"), json(i));
      }

      for (int i = 0; i < 100; i++) {
        boolean isOk =
            blobStore.put(writer, "second_dataset", "row_" + i, Sets.newHashSet("DS_2"), json(i));
      }

      for (int i = 0; i < 100; i++) {
        boolean isOk = blobStore.put(writer, "third_dataset", "row_" + i,
            Sets.newHashSet("DS_1", "DS_2"), json(i));
      }
    }
  }

  private int count(BlobStore blobStore, String dataset, Authorizations authorizations) {

    Preconditions.checkNotNull(blobStore, "blobStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (BatchScanner scanner = blobStore.batchScanner(authorizations)) {
      return Iterators.size(blobStore.get(scanner, dataset));
    }
  }

  private String json(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }";
  }
}
