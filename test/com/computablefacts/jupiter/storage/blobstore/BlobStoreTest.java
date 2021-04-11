package com.computablefacts.jupiter.storage.blobstore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Data;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BlobStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void addLocalityGroup() throws Exception {

    File file = Data.file(10);
    String str = Codecs.asString(Data.json(1));
    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, "1", labels, file));
      Assert.assertTrue(blobStore.putString(writer, dataset, "2", labels, str));
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    Map<String, Set<Text>> groupsBefore = Tables
        .getLocalityGroups(blobStore.configurations().tableOperations(), blobStore.tableName());

    Assert.assertEquals(0, groupsBefore.size());

    Assert.assertTrue(blobStore.addLocalityGroup(dataset));

    Map<String, Set<Text>> groupsAfter = Tables
        .getLocalityGroups(blobStore.configurations().tableOperations(), blobStore.tableName());

    Assert.assertEquals(1, groupsAfter.size());
    Assert.assertEquals(Sets.newHashSet(new Text(dataset)), groupsAfter.get(dataset));
  }

  @Test
  public void testRemoveDataset() throws Exception {

    File file = Data.file(10);
    String str = Codecs.asString(Data.json(1));
    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, "1", labels, file));
      Assert.assertTrue(blobStore.putString(writer, dataset, "2", labels, str));
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeDataset(deleter, dataset));
    }

    try (BatchScanner scanner = blobStore.batchScanner(auths, 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset).forEachRemaining(blobs::add);

      Assert.assertTrue(blobs.isEmpty());
    }
  }

  @Test
  public void testRemoveKeys() throws Exception {

    File file = Data.file(10);
    String str = Codecs.asString(Data.json(1));
    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, "1", labels, file));
      Assert.assertTrue(blobStore.putString(writer, dataset, "2", labels, str));
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchDeleter deleter = blobStore.deleter(auths)) {
      Assert.assertTrue(blobStore.removeKeys(deleter, dataset, Sets.newHashSet("2")));
    }

    try (BatchScanner scanner = blobStore.batchScanner(auths, 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset).forEachRemaining(blobs::add);

      Assert.assertEquals(2, blobs.size());

      checkFile(blobs.get(0), file);
      checkJson(blobs.get(1));
    }
  }

  @Test
  public void testPutAndGetFile() throws Exception {

    File file = Data.file(10);
    String dataset = "blobs";
    String bucketId = "1";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, bucketId, labels, file));
    }

    try (Scanner scanner = blobStore.scanner(auths)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, bucketId).forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      checkFile(blobs.get(0), file);
    }
  }

  @Test
  public void testPutAndGetString() throws Exception {

    String str = Codecs.asString(Data.json(1));
    String dataset = "blobs";
    String bucketId = "2";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putString(writer, dataset, bucketId, labels, str));
    }

    try (Scanner scanner = blobStore.scanner(auths)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, bucketId).forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      checkString(blobs.get(0));
    }
  }

  @Test
  public void testPutAndGetJson() throws Exception {

    Map<String, Object> json = Data.json(1);
    String dataset = "blobs";
    String bucketId = "3";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putJson(writer, dataset, bucketId, labels, json));
    }

    try (Scanner scanner = blobStore.scanner(auths)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, bucketId).forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      checkJson(blobs.get(0));
    }
  }

  @Test
  public void testPutAndGetManyBlobs() throws Exception {

    File file = Data.file(10);
    String str = Codecs.asString(Data.json(1));
    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, "1", labels, file));
      Assert.assertTrue(blobStore.putString(writer, dataset, "2", labels, str));
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchScanner scanner = blobStore.batchScanner(auths, 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, Sets.newHashSet("1", "3"), null).forEachRemaining(blobs::add);

      Assert.assertEquals(2, blobs.size());

      checkFile(blobs.get(0), file);
      checkJson(blobs.get(1));
    }
  }

  @Test
  public void testPutAndGetAllBlobs() throws Exception {

    File file = Data.file(10);
    String str = Codecs.asString(Data.json(1));
    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putFile(writer, dataset, "1", labels, file));
      Assert.assertTrue(blobStore.putString(writer, dataset, "2", labels, str));
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchScanner scanner = blobStore.batchScanner(auths, 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset).forEachRemaining(blobs::add);

      Assert.assertEquals(3, blobs.size());

      checkFile(blobs.get(0), file);
      checkString(blobs.get(1));
      checkJson(blobs.get(2));
    }
  }

  @Test
  public void testFilterOutJsonFields() throws Exception {

    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchScanner scanner =
        blobStore.batchScanner(new Authorizations("ADM", "BLOBS_RAW_DATA"), 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, null, Sets.newHashSet("Actors[*]¤name", "Actors[*]¤age"))
          .forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isJson());
      Assert.assertEquals("blobs", blob.dataset());
      Assert.assertEquals("3", blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(Codecs.asObject(
          "{\"Actors\":[{\"name\":\"Tom Cruise\",\"age\":56},{\"name\":\"Robert Downey Jr.\",\"age\":73}]}"),
          Codecs.asObject(blob.value().toString()));
    }
  }

  @Test
  public void testAnonymizeJsonFields() throws Exception {

    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths =
        new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_RAW_FILE", "BLOBS_ACTORS_NAME");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putJson(writer, dataset, "3", labels, json));
    }

    try (BatchScanner scanner =
        blobStore.batchScanner(new Authorizations("ADM", "BLOBS_ACTORS_NAME"), 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, null, Sets.newHashSet("Actors[*]¤name", "Actors[*]¤age"))
          .forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isJson());
      Assert.assertEquals("blobs", blob.dataset());
      Assert.assertEquals("3", blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(
          Codecs.asObject(
              "{\"Actors\":[{\"name\":\"Tom Cruise\"},{\"name\":\"Robert Downey Jr.\"}]}"),
          Codecs.asObject(blob.value().toString()));
    }
  }

  private BlobStore newBlobStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newBlobStore(auths, username);
  }

  private BlobStore newBlobStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    BlobStore blobStore = new BlobStore(configurations, tableName);

    Assert.assertTrue(blobStore.create());

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return blobStore;
  }

  private void checkFile(Blob<Value> blob, File file) {
    Assert.assertTrue(blob.isFile());
    Assert.assertEquals("blobs", blob.dataset());
    Assert.assertEquals("1", blob.key());
    Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_FILE"), blob.labels());
    Assert.assertEquals(Lists.newArrayList(file.getName(), "10"), blob.properties());
    Assert.assertEquals(new Value(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), blob.value());
  }

  private void checkString(Blob<Value> blob) {
    Assert.assertTrue(blob.isString());
    Assert.assertEquals("blobs", blob.dataset());
    Assert.assertEquals("2", blob.key());
    Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
    Assert.assertEquals(Lists.newArrayList(), blob.properties());
    Assert.assertEquals(Codecs.asString(Data.json(1)), blob.value().toString());
  }

  private void checkJson(Blob<Value> blob) {
    Assert.assertTrue(blob.isJson());
    Assert.assertEquals("blobs", blob.dataset());
    Assert.assertEquals("3", blob.key());
    Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
    Assert.assertEquals(Lists.newArrayList(), blob.properties());
    Assert.assertEquals(Data.json(1), Codecs.asObject(blob.value().toString()));
  }
}
