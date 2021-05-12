package com.computablefacts.jupiter.iterators;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.base.JacksonJsonCore;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreFilterOutJsonFieldsIterator
    implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

  private static final String FIELDS_CRITERION = "f";

  private SortedKeyValueIterator<Key, Value> source_;
  private Map<String, String> options_;
  private Key topKey_;
  private Value topValue_;
  private Set<String> keepFields_;
  private ObjectMapper mapper_;
  private JacksonJsonCore jsonCore_;

  public BlobStoreFilterOutJsonFieldsIterator() {}

  public static void setFieldsToKeep(IteratorSetting setting, Set<String> fields) {
    if (fields != null) {
      setting.addOption(FIELDS_CRITERION, Joiner.on(SEPARATOR_NUL).join(fields));
    }
  }

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(FIELDS_CRITERION, "Fields patterns to keep.");

    return new IteratorOptions("BlobStoreFilterOutJsonFieldsIterator",
        "BlobStoreFilterOutJsonFieldsIterator filters out the JSON fields stored in the Accumulo Value the user did not explicitly asked for.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return options.containsKey(FIELDS_CRITERION) && options.get(FIELDS_CRITERION) != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment environment) {
    source_ = source;
    options_ = new HashMap<>(options);
    keepFields_ = options.containsKey(FIELDS_CRITERION)
        ? Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(options.get(FIELDS_CRITERION)))
        : null;
    mapper_ = new ObjectMapper();
    mapper_.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    jsonCore_ = new JacksonJsonCore(mapper_);
  }

  @Override
  public boolean hasTop() {
    return topKey_ != null;
  }

  @Override
  public void next() throws IOException {
    if (source_.hasTop()) {
      setTopKeyValue(source_.getTopKey(), source_.getTopValue());
      source_.next();
    } else {
      topKey_ = null;
      topValue_ = null;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source_.seek(range, columnFamilies, inclusive);
    next();
  }

  @Override
  public Key getTopKey() {
    return topKey_;
  }

  protected void setTopKey(Key key) {
    topKey_ = new Key(key);
  }

  @Override
  public Value getTopValue() {
    return topValue_;
  }

  protected void setTopValue(Value value) {
    topValue_ = new Value(value);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment environment) {
    BlobStoreFilterOutJsonFieldsIterator iterator = new BlobStoreFilterOutJsonFieldsIterator();
    iterator.init(source_.deepCopy(environment), options_, environment);
    return iterator;
  }

  private void setTopKeyValue(Key key, Value value) {

    setTopKey(key);

    if (key.getColumnQualifier() == null || !Blob.isJson(key)) {
      setTopValue(value);
    } else {

      Map<String, Object> json = new JsonFlattener(jsonCore_, value.toString())
          .withSeparator(SEPARATOR_CURRENCY_SIGN).flattenAsMap();

      // First, remove all fields that have not been explicitly asked for
      json.keySet().removeIf(field -> !acceptField(field));

      // Then, rebuild a new JSON object
      String newJson =
          new JsonUnflattener(jsonCore_, json).withSeparator(SEPARATOR_CURRENCY_SIGN).unflatten();

      // Next, set the new JSON object as the new Accumulo Value
      setTopValue(new Value(newJson.getBytes(StandardCharsets.UTF_8)));
    }
  }

  private boolean acceptField(String field) {
    for (String pattern : keepFields_) {
      if (WildcardMatcher.match(field, pattern)) {
        return true;
      }
    }
    return false;
  }
}
