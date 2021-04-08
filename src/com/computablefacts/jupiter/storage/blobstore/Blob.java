package com.computablefacts.jupiter.storage.blobstore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_PIPE;

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

  private static final int TYPE_UNKNOWN = 0;
  private static final int TYPE_STRING = 1;
  private static final int TYPE_FILE = 2;
  private static final int TYPE_JSON = 3;

  private final String dataset_;
  private final String key_;
  private final Set<String> labels_;
  private final int type_;
  private final List<String> properties_;
  private final T value_;

  Blob(String dataset, String key, Set<String> labels, int type, T value, List<String> properties) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
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
    return key != null && key.getColumnQualifier() != null
        && key.getColumnQualifier().toString().startsWith(TYPE_JSON + "" + SEPARATOR_NUL);
  }

  public static Mutation fromString(String dataset, String key, Set<String> labels, String value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return newMutation(dataset, key, labels, TYPE_STRING, value.getBytes(StandardCharsets.UTF_8),
        null);
  }

  public static Mutation fromJson(String dataset, String key, Set<String> labels, String value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    return newMutation(dataset, key, labels, TYPE_JSON, value.getBytes(StandardCharsets.UTF_8),
        null);
  }

  public static Mutation fromFile(String dataset, String key, Set<String> labels,
      java.io.File file) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(file, "file should not be null");
    Preconditions.checkArgument(file.exists(), "Missing file : %s", file);

    try {
      return newMutation(dataset, key, labels, TYPE_FILE,
          java.nio.file.Files.readAllBytes(file.toPath()),
          Lists.newArrayList(file.getName(), Long.toString(file.length(), 10)));
    } catch (IOException e) {
      // TODO
    }
    return null;
  }

  public static Blob<Value> fromKeyValue(Key key, Value value) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    String row = key.getRow().toString(); // blob identifier
    String cf = key.getColumnFamily().toString(); // dataset
    String cq = key.getColumnQualifier().toString();
    String cv = key.getColumnVisibility().toString();

    // Extract visibility labels from CV
    Set<String> labels =
        Sets.newHashSet(Splitter.on(SEPARATOR_PIPE).trimResults().omitEmptyStrings().split(cv));

    // Extract blob's type from CQ (legacy)
    int index = cq.indexOf(SEPARATOR_NUL);
    if (index < 0) {
      return new Blob<>(cf, row, labels, TYPE_UNKNOWN, value, Lists.newArrayList());
    }

    // Extract misc. blob's properties from CQ
    List<String> properties =
        Splitter.on(SEPARATOR_NUL).trimResults().omitEmptyStrings().splitToList(cq);

    // Extract blob's type from CQ
    int type = Integer.parseInt(properties.get(0), 10);

    return new Blob<>(cf, row, labels, type, value, properties.subList(1, properties.size()));
  }

  private static Mutation newMutation(String dataset, String key, Set<String> labels, int type,
      byte[] bytes, List<String> properties) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(bytes, "bytes should not be null");

    StringBuilder cq = new StringBuilder();
    cq.append(type);
    cq.append(SEPARATOR_NUL);

    if (properties != null && !properties.isEmpty()) {
      cq.append(Joiner.on(SEPARATOR_NUL).join(properties));
    }

    ColumnVisibility cv = new ColumnVisibility(Joiner.on(SEPARATOR_PIPE).join(labels));

    Mutation mutation = new Mutation(key);
    mutation.put(new Text(dataset), new Text(cq.toString()), cv, new Value(bytes));

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
  public int type() {
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
  public boolean isUnknown() {
    return type_ == TYPE_UNKNOWN;
  }

  @Generated
  public boolean isString() {
    return type_ == TYPE_STRING;
  }

  @Generated
  public boolean isJson() {
    return type_ == TYPE_JSON;
  }

  @Generated
  public boolean isFile() {
    return type_ == TYPE_FILE;
  }
}
