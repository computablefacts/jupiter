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
  public void testAddLocalityGroup() throws Exception {

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
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_FILE");
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
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA");
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
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA");
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
    String key = "3";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putJson(writer, dataset, key, labels, json));
    }

    try (BatchScanner scanner =
        blobStore.batchScanner(new Authorizations("ADM", "BLOBS_RAW_DATA"), 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset, null, Sets.newHashSet("Actors[*]¤name", "Actors[*]¤age"))
          .forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isJson());
      Assert.assertEquals(dataset, blob.dataset());
      Assert.assertEquals(key, blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(Codecs.asObject(
          "{\"Actors\":[{\"name\":\"Tom Cruise\",\"age\":56},{\"name\":\"Robert Downey Jr.\",\"age\":73}]}"),
          Codecs.asObject(blob.value().toString()));
    }
  }

  @Test
  public void testAnonymizeJson() throws Exception {

    Map<String, Object> json = Data.json(1);

    String dataset = "blobs";
    String key = "3";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA", "BLOBS_ACTORS_NAME");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putJson(writer, dataset, key, labels, json));
    }

    try (BatchScanner scanner =
        blobStore.batchScanner(new Authorizations("ADM", "BLOBS_ACTORS_NAME"), 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset).forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isJson());
      Assert.assertEquals(dataset, blob.dataset());
      Assert.assertEquals(key, blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(Codecs.asObject(
          "{\"Actors\":[{\"photo\":\"ANONYMIZED_563c13a31f5931533db40a912a41b5d9\", \"weight\":\"ANONYMIZED_4103e8509cbdf6b3372222061bbe1da6\", \"Birthdate\":\"ANONYMIZED_d7646b9ba6826d72e61c7bcff956d8cc\", \"children\":[\"ANONYMIZED_0e7c75229e51ea2fe77dc9700584d7a9\", \"ANONYMIZED_117c53a952a95ad97ec56b47eb90e961\", \"ANONYMIZED_ee47baa57443c38425f3a5cadcdcdd53\"], \"wife\":\"ANONYMIZED_00000000000000000000000000000000\", \"hasChildren\":\"ANONYMIZED_5db32d6ecc1f5ef816ebe6268a3343c2\", \"name\":\"Tom Cruise\", \"age\":\"ANONYMIZED_0552cbaa70e872fd23b6bb12a808f8ec\", \"hasGreyHair\":\"ANONYMIZED_e495b7e5056dbfc4e854950696d4c3cc\", \"Born At\":\"ANONYMIZED_63091514f7bd5d66ca9712d54a348004\", \"uuid\":\"ANONYMIZED_279eb7ef59306bba9ea28f4d55bebc2a\"}, {\"Birthdate\":\"ANONYMIZED_2077f9af8388b86889a3cc8be63cfa90\", \"wife\":\"ANONYMIZED_b0c0dc7f2f6d36ab282f8b2a4a0fa5c7\", \"weight\":\"ANONYMIZED_0e13006e4837c9e38fa953851c92feec\", \"photo\":\"ANONYMIZED_2a6cddab26138fe42174505d416bfd6f\", \"Born At\":\"ANONYMIZED_a88ac179dc9144029a8292a75e1c298c\", \"children\":[\"ANONYMIZED_2fabdc904c6fa394ba1a3ae0331011eb\", \"ANONYMIZED_7d0caf4910b779d7ac1fce0af089939b\", \"ANONYMIZED_f8da0993fdca133f8a3cf6ee44e11087\"], \"hasChildren\":\"ANONYMIZED_5db32d6ecc1f5ef816ebe6268a3343c2\", \"hasGreyHair\":\"ANONYMIZED_e495b7e5056dbfc4e854950696d4c3cc\", \"age\":\"ANONYMIZED_3974c437d717863985a0b5618f289b46\", \"uuid\":\"ANONYMIZED_715931ea5ce0f15e1a0a2afad3ef4a52\", \"name\":\"Robert Downey Jr.\"}], \"uuid\":\"ANONYMIZED_717c7b8afebbfb7137f6f0f99beb2a94\"}"),
          Codecs.asObject(blob.value().toString()));
    }
  }

  @Test
  public void testAnonymizeBlob() throws Exception {

    String str = Codecs.asString(Data.json(1));

    String dataset = "blobs";
    String key = "2";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "BLOBS_RAW_DATA");
    BlobStore blobStore = newBlobStore(auths);

    try (BatchWriter writer = blobStore.writer()) {
      Assert.assertTrue(blobStore.putString(writer, dataset, key, labels, str));
    }

    try (BatchScanner scanner = blobStore.batchScanner(new Authorizations("ADM"), 1)) {

      List<Blob<Value>> blobs = new ArrayList<>();
      blobStore.get(scanner, dataset).forEachRemaining(blobs::add);

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isString());
      Assert.assertEquals(dataset, blob.dataset());
      Assert.assertEquals(key, blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(Codecs.asObject("{}"), Codecs.asObject(blob.value().toString()));
    }
  }

  private BlobStore newBlobStore(Authorizations auths) throws Exception {

    String username = nextUsername();
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
