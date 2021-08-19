package com.computablefacts.jupiter.storage.blobstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.STRING_RAW_DATA;
import static com.computablefacts.jupiter.storage.Constants.STRING_RAW_FILE;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_ARRAY;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_FILE;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_JSON;
import static com.computablefacts.jupiter.storage.blobstore.BlobStore.TYPE_STRING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.nona.Generated;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Blob<T> {

  private final String dataset_;
  private final String key_;
  private final Set<String> labels_;
  private final String type_;
  private final List<String> properties_;
  private final T value_;

  Blob(String dataset, String key, Set<String> labels, String type, T value,
      List<String> properties) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(type, "type should not be null");
    Preconditions.checkNotNull(value, "value should not be null");
    Preconditions.checkNotNull(properties, "properties should not be null");

    dataset_ = dataset;
    key_ = key;
    type_ = type;
    value_ = value;
    labels_ = new HashSet<>(labels);
    properties_ = new ArrayList<>(properties);
  }

  public static boolean isJson(Key key) {
    return key != null && key.getColumnFamily() != null
        && key.getColumnFamily().toString().equals(TYPE_JSON);
  }

  public static boolean isString(Key key) {
    return key != null && key.getColumnFamily() != null
        && key.getColumnFamily().toString().equals(TYPE_STRING);
  }

  public static boolean isFile(Key key) {
    return key != null && key.getColumnFamily() != null
        && key.getColumnFamily().toString().equals(TYPE_FILE);
  }

  public static boolean isArray(Key key) {
    return key != null && key.getColumnFamily() != null
        && key.getColumnFamily().toString().equals(TYPE_ARRAY);
  }

  public static Mutation fromString(String dataset, String key, Set<String> labels, String value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return newMutation(dataset, key, new HashSet<>(labels), TYPE_STRING,
        value.getBytes(StandardCharsets.UTF_8), null);
  }

  public static Mutation fromJson(String dataset, String key, Set<String> labels, String value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return newMutation(dataset, key, new HashSet<>(labels), TYPE_JSON,
        value.getBytes(StandardCharsets.UTF_8), null);
  }

  public static Mutation fromFile(String dataset, String key, Set<String> labels,
      java.io.File file) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(file, "file should not be null");
    Preconditions.checkArgument(file.exists(), "Missing file : %s", file);

    try {
      return newMutation(dataset, key, new HashSet<>(labels), TYPE_FILE,
          java.nio.file.Files.readAllBytes(file.toPath()),
          Lists.newArrayList(file.getName(), Long.toString(file.length(), 10)));
    } catch (IOException e) {
      // TODO
    }
    return null;
  }

  public static Mutation fromArray(String dataset, String key, Set<String> labels, String value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return newMutation(dataset, key, new HashSet<>(labels), TYPE_ARRAY,
        value.getBytes(StandardCharsets.UTF_8), null);
  }

  public static Blob<Value> fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString(); // dataset\0blob identifier
    String cf = key.getColumnFamily().toString();
    String cq = key.getColumnQualifier().toString();
    String cv = key.getColumnVisibility().toString();

    // Extract visibility labels from CV
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract dataset from ROW
    int index = row.indexOf(SEPARATOR_NUL);
    String dataset = row.substring(0, index);
    String identifier = row.substring(index + 1);

    // Extract misc. blob's properties from CQ
    List<String> properties =
        Splitter.on(SEPARATOR_NUL).trimResults().omitEmptyStrings().splitToList(cq);

    return new Blob<>(dataset, identifier, labels, cf, value, properties);
  }

  private static Mutation newMutation(String dataset, String key, Set<String> labels, String type,
      byte[] bytes, List<String> properties) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(type, "type should not be null");
    Preconditions.checkNotNull(bytes, "bytes should not be null");

    String cq;

    if (properties != null && !properties.isEmpty()) {
      cq = Joiner.on(SEPARATOR_NUL).join(properties);
    } else {
      cq = "";
    }

    labels.add(STRING_ADM);

    if (TYPE_FILE.equals(type)) {
      labels.add(AbstractStorage.toVisibilityLabel(dataset + "_" + STRING_RAW_FILE));
    } else {
      labels.add(AbstractStorage.toVisibilityLabel(dataset + "_" + STRING_RAW_DATA));
    }

    ColumnVisibility cv = new ColumnVisibility(Joiner.on(SEPARATOR_PIPE).join(labels));

    Mutation mutation = new Mutation(dataset + SEPARATOR_NUL + key);
    mutation.put(new Text(type), new Text(cq), cv, new Value(bytes));

    return mutation;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataset", dataset_).add("key", key_)
        .add("labels", labels_).add("type", type_).add("properties", properties_)
        .add("value", value_).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Blob)) {
      return false;
    }
    Blob<?> blob = (Blob<?>) obj;
    return Objects.equal(dataset_, blob.dataset_) && Objects.equal(key_, blob.key_)
        && Objects.equal(labels_, blob.labels_) && Objects.equal(type_, blob.type_)
        && Objects.equal(properties_, blob.properties_) && Objects.equal(value_, blob.value_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset_, key_, labels_, type_, properties_, value_);
  }

  @Generated
  public String dataset() {
    return dataset_;
  }

  @Generated
  public String key() {
    return key_;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public String type() {
    return type_;
  }

  @Generated
  public T value() {
    return value_;
  }

  @Generated
  public List<String> properties() {
    return properties_;
  }

  @Generated
  public boolean isString() {
    return TYPE_STRING.equals(type_);
  }

  @Generated
  public boolean isJson() {
    return TYPE_JSON.equals(type_);
  }

  @Generated
  public boolean isFile() {
    return TYPE_FILE.equals(type_);
  }

  @Generated
  public boolean isArray() {
    return TYPE_ARRAY.equals(type_);
  }
}
