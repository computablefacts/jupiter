package com.computablefacts.jupiter.storage.blobstore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

/**
 * This class is not thread-safe because {@link MiniAccumuloClusterUtils#setUserAuths} is used. Do
 * not execute methods in parallel.
 */
@net.jcip.annotations.NotThreadSafe
public class BlobStoreTest {

  private static MiniAccumuloCluster accumulo;
  private static Configurations configurations;
  private static BlobStore blobStore;

  @BeforeClass
  public static void initClass() throws Exception {
    accumulo = MiniAccumuloClusterUtils.newCluster();
    configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    blobStore = new BlobStore(configurations, "blobs");
  }

  @AfterClass
  public static void uinitClass() throws Exception {
    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private static int countFirst(Authorizations authorizations) {
    return count("first_dataset", authorizations);
  }

  private static int countSecond(Authorizations authorizations) {
    return count("second_dataset", authorizations);
  }

  private static int countThird(Authorizations authorizations) {
    return count("third_dataset", authorizations);
  }

  private static int count(String dataset, Authorizations authorizations) {
    return all(dataset, authorizations).size();
  }

  private static List<Blob<Value>> firstDataset(Authorizations authorizations) {
    return all("first_dataset", authorizations);
  }

  private static List<Blob<Value>> secondDataset(Authorizations authorizations) {
    return all("second_dataset", authorizations);
  }

  private static List<Blob<Value>> thirdDataset(Authorizations authorizations) {
    return all("third_dataset", authorizations);
  }

  private static List<Blob<Value>> all(String dataset, Authorizations authorizations) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanner scanner = blobStore.scanner(authorizations)) { // keep order

      List<Blob<Value>> list = new ArrayList<>();
      Iterator<Blob<Value>> iterator = blobStore.get(scanner, dataset);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private static void fill() throws Exception {

    Preconditions.checkNotNull(blobStore, "blobStore should not be null");

    try (BatchWriter writer = blobStore.writer()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            blobStore.put(writer, "first_dataset", "row_" + i, Sets.newHashSet("DS_1"), json(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            blobStore.put(writer, "second_dataset", "row_" + i, Sets.newHashSet("DS_2"), json(i)));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(blobStore.put(writer, "third_dataset", "row_" + i,
            Sets.newHashSet("DS_1", "DS_2"), json(i)));
      }
    }
  }

  private static String json(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }";
  }

  @Before
  public void initMethods() throws Exception {
    if (blobStore.isReady()) {
      boolean isOk = blobStore.destroy();
    }
    if (blobStore.create()) {
      fill();
    }
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    Assert.assertTrue(blobStore.create()); // ensure create is reentrant
    Assert.assertTrue(blobStore.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    BlobStore blobStore = new BlobStore(configurations, "blobs");

    Assert.assertTrue(blobStore.create());
    Assert.assertTrue(blobStore.isReady());

    Assert.assertTrue(blobStore.destroy());
    Assert.assertFalse(blobStore.isReady());

    Assert.assertTrue(blobStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(blobStore.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, new Authorizations("DS_1", "DS_2"));

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    Assert.assertTrue(blobStore.truncate());

    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(0, countThird(auths));
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeDataset(deleter, "first_dataset"));
      Assert.assertTrue(blobStore.removeDataset(deleter, "second_dataset"));
    }

    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
  }

  @Test
  public void testRemoveBlobs() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    Set<String> odd = new HashSet<>();
    Set<String> even = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) { // remove even rows from dataset 1
        even.add("row_" + i);
      } else { // remove odd rows from dataset 2
        odd.add("row_" + i);
      }
    }

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeKeys(deleter, "first_dataset", even));
      Assert.assertTrue(blobStore.removeKeys(deleter, "second_dataset", odd));
    }

    Assert.assertEquals(5, countFirst(auths));
    Assert.assertEquals(5, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    // Ensure odd rows remain in dataset 1
    List<Blob<Value>> list1 = firstDataset(auths);

    for (Blob<Value> blob : list1) {
      Assert.assertEquals(1, Integer.parseInt(blob.key().substring("row_".length()), 10) % 2);
    }

    // Ensure even rows remain in dataset 2
    List<Blob<Value>> list2 = secondDataset(auths);

    for (Blob<Value> blob : list2) {
      Assert.assertEquals(0, Integer.parseInt(blob.key().substring("row_".length()), 10) % 2);
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    Assert.assertEquals(10, countFirst(authsDS1));
    Assert.assertEquals(10, countFirst(authsDS2)); // Throws an exception
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1DS2);

    Assert.assertEquals(10, countFirst(authsDS1DS2));
    Assert.assertEquals(10, countSecond(authsDS1DS2));
    Assert.assertEquals(10, countThird(authsDS1DS2));

    Assert.assertEquals(10, countFirst(authsDS1));
    Assert.assertEquals(0, countSecond(authsDS1));
    Assert.assertEquals(10, countThird(authsDS1));

    Assert.assertEquals(0, countFirst(authsDS2));
    Assert.assertEquals(10, countSecond(authsDS2));
    Assert.assertEquals(10, countThird(authsDS2));
  }

  @Test
  public void testScanners() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    try (Scanner scanner = blobStore.scanner(authsDS1)) { // keep order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }

    try (BatchScanner scanner = blobStore.batchScanner(authsDS1)) { // out of order

      Assert.assertTrue(AbstractStorage.setRange(scanner, new Range()));
      scanner.fetchColumnFamily(new Text("first_dataset"));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

      Assert.assertEquals(10, Iterators.size(iterator));
    }
  }

  @Test
  public void testGetOneBlob() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (BatchScanner scanner = blobStore.batchScanner(auths)) { // out of order

      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset", "row_1");
      Blob<Value> blob = iterator.next();

      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("row_1", blob.key());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
      Assert.assertEquals(json(1), blob.value().toString());
    }
  }

  @Test
  public void testGetMoreThanOneBlob() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = blobStore.scanner(auths)) { // keep order

      @Var
      int i = 0;
      Iterator<Blob<Value>> iterator = blobStore.get(scanner, "third_dataset");

      while (iterator.hasNext()) {

        Blob<Value> blob = iterator.next();

        Assert.assertEquals("row_" + i, blob.key());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), blob.labels());
        Assert.assertEquals(json(i), blob.value().toString());

        i++;
      }
    }
  }

  @Test
  public void testAddLocalityGroup() {

    Assert.assertTrue(
        Tables.getLocalityGroups(blobStore.configurations().tableOperations(), "blobs").isEmpty());

    Assert.assertTrue(blobStore.addLocalityGroup("third_dataset"));
    Assert.assertFalse(
        Tables.getLocalityGroups(blobStore.configurations().tableOperations(), "blobs").isEmpty());

    Assert.assertTrue(blobStore.addLocalityGroup("third_dataset")); // ensure reentrant
    Assert.assertFalse(
        Tables.getLocalityGroups(blobStore.configurations().tableOperations(), "blobs").isEmpty());
  }
}
