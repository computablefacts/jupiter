package com.computablefacts.jupiter.storage.blobstore;

import java.io.File;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Data;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.helpers.RandomString;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class BlobStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testArrayShard() {

    RandomString ticketsGenerator =
        new RandomString(5, new SecureRandom(), RandomString.digits + RandomString.lower);

    for (int i = 0; i < 1000 /* how many times the test should be run */; i++) {

      long[][] table = new long[2][100];

      for (int k = 0; k < 100 /* MAX_NUMBER_OF_SHARDS */; k++) {
        table[0][k] = 1;
      }

      for (int k = 0; k < 100 /* MAX_NUMBER_OF_SHARDS */; k++) {
        String key =
            ticketsGenerator.nextString() + Constants.SEPARATOR_PIPE + Instant.now().toString();
        String shard = BlobStore.arrayShard(key);
        table[1][Integer.parseInt(shard.substring("ARR".length()), 10)]++;
      }

      double alpha = 0.01; // confidence level 99%
      ChiSquareTest test = new ChiSquareTest();

      double pval = test.chiSquareTest(table);
      System.out.printf("p-value: %.9f\n", pval);

      boolean rejected = test.chiSquareTest(table, alpha);
      System.out.println("X^2 Test: " + ((!rejected) ? ("PASS") : ("FAIL")));

      Assert.assertFalse(rejected);
    }
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

    Assert.assertTrue(blobStore.removeDataset(dataset));

    try (BatchScanner scanner = blobStore.batchScanner(auths, 1)) {

      @Var
      List<Blob<Value>> blobs = blobStore.getFiles(scanner, dataset, null, null).toList();

      Assert.assertTrue(blobs.isEmpty());

      blobs = blobStore.getStrings(scanner, dataset, null, null).toList();

      Assert.assertTrue(blobs.isEmpty());

      blobs = blobStore.getJsons(scanner, dataset, null, null).toList();

      Assert.assertTrue(blobs.isEmpty());
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

      List<Blob<Value>> blobs =
          blobStore.getFiles(scanner, dataset, Sets.newHashSet(bucketId), null).toList();

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

      List<Blob<Value>> blobs =
          blobStore.getStrings(scanner, dataset, Sets.newHashSet(bucketId), null).toList();

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

      List<Blob<Value>> blobs =
          blobStore.getJsons(scanner, dataset, Sets.newHashSet(bucketId), null).toList();

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

      @Var
      List<Blob<Value>> blobs =
          blobStore.getJsons(scanner, dataset, Sets.newHashSet("1", "3"), null).toList();

      Assert.assertEquals(1, blobs.size());

      checkJson(blobs.get(0));

      blobs = blobStore.getFiles(scanner, dataset, Sets.newHashSet("1", "3"), null).toList();

      Assert.assertEquals(1, blobs.size());

      checkFile(blobs.get(0), file);

      blobs = blobStore.getStrings(scanner, dataset, Sets.newHashSet("1", "3"), null).toList();

      Assert.assertEquals(0, blobs.size());
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

      @Var
      List<Blob<Value>> blobs = blobStore.getJsons(scanner, dataset, null, null).toList();

      Assert.assertEquals(1, blobs.size());

      checkJson(blobs.get(0));

      blobs = blobStore.getStrings(scanner, dataset, null, null).toList();

      Assert.assertEquals(1, blobs.size());

      checkString(blobs.get(0));

      blobs = blobStore.getFiles(scanner, dataset, null, null).toList();

      Assert.assertEquals(1, blobs.size());

      checkFile(blobs.get(0), file);
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

      List<Blob<Value>> blobs = blobStore
          .getJsons(scanner, dataset, null, Sets.newHashSet("Actors[*]¤name", "Actors[*]¤age"))
          .toList();

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

      List<Blob<Value>> blobs = blobStore.getJsons(scanner, dataset, null, null).toList();

      Assert.assertEquals(1, blobs.size());

      Blob<Value> blob = blobs.get(0);

      Assert.assertTrue(blob.isJson());
      Assert.assertEquals(dataset, blob.dataset());
      Assert.assertEquals(key, blob.key());
      Assert.assertEquals(Sets.newHashSet("ADM", "BLOBS_RAW_DATA"), blob.labels());
      Assert.assertEquals(Lists.newArrayList(), blob.properties());
      Assert.assertEquals(Codecs.asObject(
          "{\"Actors\":[{\"photo\":\"MASKED_563c13a31f5931533db40a912a41b5d9\", \"weight\":\"MASKED_4103e8509cbdf6b3372222061bbe1da6\", \"Birthdate\":\"MASKED_d7646b9ba6826d72e61c7bcff956d8cc\", \"children\":[\"MASKED_0e7c75229e51ea2fe77dc9700584d7a9\", \"MASKED_117c53a952a95ad97ec56b47eb90e961\", \"MASKED_ee47baa57443c38425f3a5cadcdcdd53\"], \"wife\":\"MASKED_00000000000000000000000000000000\", \"hasChildren\":\"MASKED_5db32d6ecc1f5ef816ebe6268a3343c2\", \"name\":\"Tom Cruise\", \"age\":\"MASKED_0552cbaa70e872fd23b6bb12a808f8ec\", \"hasGreyHair\":\"MASKED_e495b7e5056dbfc4e854950696d4c3cc\", \"Born At\":\"MASKED_63091514f7bd5d66ca9712d54a348004\", \"uuid\":\"MASKED_279eb7ef59306bba9ea28f4d55bebc2a\"}, {\"Birthdate\":\"MASKED_2077f9af8388b86889a3cc8be63cfa90\", \"wife\":\"MASKED_b0c0dc7f2f6d36ab282f8b2a4a0fa5c7\", \"weight\":\"MASKED_0e13006e4837c9e38fa953851c92feec\", \"photo\":\"MASKED_2a6cddab26138fe42174505d416bfd6f\", \"Born At\":\"MASKED_a88ac179dc9144029a8292a75e1c298c\", \"children\":[\"MASKED_2fabdc904c6fa394ba1a3ae0331011eb\", \"MASKED_7d0caf4910b779d7ac1fce0af089939b\", \"MASKED_f8da0993fdca133f8a3cf6ee44e11087\"], \"hasChildren\":\"MASKED_5db32d6ecc1f5ef816ebe6268a3343c2\", \"hasGreyHair\":\"MASKED_e495b7e5056dbfc4e854950696d4c3cc\", \"age\":\"MASKED_3974c437d717863985a0b5618f289b46\", \"uuid\":\"MASKED_715931ea5ce0f15e1a0a2afad3ef4a52\", \"name\":\"Robert Downey Jr.\"}], \"uuid\":\"MASKED_717c7b8afebbfb7137f6f0f99beb2a94\"}"),
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

      List<Blob<Value>> blobs = blobStore.getStrings(scanner, dataset, null, null).toList();

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

  @Test
  public void testFilterByJsonFields() throws Exception {

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

      @Var
      List<Blob<Value>> blobs = blobStore.getJsons(scanner, dataset, null, null,
          Sets.newHashSet(new AbstractMap.SimpleEntry<>("Actors[*]¤weight",
              "MASKED_4103e8509cbdf6b3372222061bbe1da6")))
          .toList();

      Assert.assertEquals(1, blobs.size());

      blobs = blobStore
          .getJsons(scanner, dataset, null, null,
              Sets.newHashSet(new AbstractMap.SimpleEntry<>("Actors[*]¤name", "Tom Cruise")))
          .toList();

      Assert.assertEquals(1, blobs.size());
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
