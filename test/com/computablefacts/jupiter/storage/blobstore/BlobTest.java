package com.computablefacts.jupiter.storage.blobstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_ARRAY;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_FILE;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_JSON;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_STRING;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.computablefacts.jupiter.Data;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BlobTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    Blob<String> blob =
        new Blob<>(null, "key", Sets.newHashSet(), TYPE_STRING, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    Blob<String> blob =
        new Blob<>("dataset", null, Sets.newHashSet(), TYPE_STRING, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Blob<String> blob =
        new Blob<>("dataset", "key", null, TYPE_STRING, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullType() {
    Blob<String> blob =
        new Blob<>("dataset", "key", Sets.newHashSet(), null, "value", Lists.newArrayList());
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperties() {
    Blob<String> blob = new Blob<>("dataset", "key", Sets.newHashSet(), TYPE_STRING, "value", null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullValue() {
    Blob<String> blob =
        new Blob<>("dataset", "key", Sets.newHashSet(), TYPE_STRING, null, Lists.newArrayList());
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Blob.class).verify();
  }

  @Test
  public void testFromString() {

    String str = Codecs.asString(Data.json(1));
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_STRING.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_RAW_DATA").getExpression();
    byte[] val = str.getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Blob.fromString("my_dataset", uuid, Sets.newHashSet(), str);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromJson() {

    Map<String, Object> json = Data.json(1);
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_JSON.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_RAW_DATA").getExpression();
    byte[] val = Codecs.asString(json).getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Blob.fromJson("my_dataset", uuid, Sets.newHashSet(), Codecs.asString(json));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromFile() throws Exception {

    File file = Data.file(10);
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_FILE.getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_RAW_FILE").getExpression();
    byte[] val = java.nio.file.Files.readAllBytes(file.toPath());

    StringBuilder cq = new StringBuilder();
    cq.append(file.getName());
    cq.append(SEPARATOR_NUL);
    cq.append(Long.toString(file.length(), 10));

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq.toString()), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Blob.fromFile("my_dataset", uuid, Sets.newHashSet(), file);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromArray() {

    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_ARRAY.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_RAW_DATA").getExpression();
    byte[] val = "value1\0value2".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Blob.fromArray("my_dataset", uuid, Sets.newHashSet(), "value1\0value2");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStringFromKeyValue() {

    String str = Codecs.asString(Data.json(1));
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_STRING.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = str.getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    Blob<Value> blob = Blob.fromKeyValue(key, value);

    Assert.assertTrue(blob.isString());
    Assert.assertEquals("my_dataset", blob.dataset());
    Assert.assertEquals(uuid, blob.key());
    Assert.assertEquals(Lists.newArrayList(), blob.properties());
    Assert.assertEquals(str, blob.value().toString());
  }

  @Test
  public void testJsonFromKeyValue() {

    Map<String, Object> json = Data.json(1);
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_JSON.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = Codecs.asString(json).getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    Blob<Value> blob = Blob.fromKeyValue(key, value);

    Assert.assertTrue(blob.isJson());
    Assert.assertEquals("my_dataset", blob.dataset());
    Assert.assertEquals(uuid, blob.key());
    Assert.assertEquals(Lists.newArrayList(), blob.properties());
    Assert.assertEquals(Data.json(1), Codecs.asObject(new String(blob.value().get())));
  }

  @Test
  public void testFileFromKeyValue() throws Exception {

    File file = Data.file(10);
    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_FILE.getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = java.nio.file.Files.readAllBytes(file.toPath());

    StringBuilder cq = new StringBuilder();
    cq.append(file.getName());
    cq.append(SEPARATOR_NUL);
    cq.append(Long.toString(file.length(), 10));

    Key key = new Key(row, cf, cq.toString().getBytes(StandardCharsets.UTF_8), cv);
    Value value = new Value(val);
    Blob<Value> blob = Blob.fromKeyValue(key, value);

    Assert.assertTrue(blob.isFile());
    Assert.assertEquals("my_dataset", blob.dataset());
    Assert.assertEquals(uuid, blob.key());
    Assert.assertEquals(Sets.newHashSet(), blob.labels());
    Assert.assertEquals(Lists.newArrayList(file.getName(), "10"), blob.properties());
    Assert.assertEquals(val, blob.value().get());
  }

  @Test
  public void testArrayFromKeyValue() {

    String uuid = UUID.randomUUID().toString();

    byte[] row = ("my_dataset\0" + uuid).getBytes(StandardCharsets.UTF_8);
    byte[] cf = TYPE_ARRAY.getBytes(StandardCharsets.UTF_8);
    byte[] cq = "".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "value1\0value2".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    Blob<Value> blob = Blob.fromKeyValue(key, value);

    Assert.assertTrue(blob.isArray());
    Assert.assertEquals("my_dataset", blob.dataset());
    Assert.assertEquals(uuid, blob.key());
    Assert.assertEquals(Sets.newHashSet(), blob.labels());
    Assert.assertEquals(Lists.newArrayList(), blob.properties());
    Assert.assertEquals(val, blob.value().get());
  }
}
