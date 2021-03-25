package com.computablefacts.jupiter.iterators;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreFilterOutJsonFieldsIterator extends AnonymizingIterator {

  private static final String TYPE_JSON = Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL;
  private static final String FIELDS_CRITERION = "f";

  private Set<String> keepFields_;

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
        "BlobStoreFilterOutJsonFieldsIterator filters out the JSON fields stored in the Accumulo Value for which the user does not have the right auths.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(FIELDS_CRITERION)) {
      return super.validateOptions(options) && options.get(FIELDS_CRITERION) != null;
    }
    return false;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) {

    super.init(source, options, env);

    keepFields_ = options.containsKey(FIELDS_CRITERION)
        ? Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(options.get(FIELDS_CRITERION)))
        : null;
  }

  @Override
  protected AnonymizingIterator create() {
    return new BlobStoreFilterOutJsonFieldsIterator();
  }

  @Override
  protected void setTopKeyValue(Key key, Value value) {

    setTopKey(key);
    setTopValue(Constants.VALUE_ANONYMIZED);

    if (!Constants.VALUE_ANONYMIZED.equals(value) && keepFields_ != null && !keepFields_.isEmpty()
        && key.getColumnQualifier() != null
        && key.getColumnQualifier().toString().startsWith(TYPE_JSON)) {

      String vizDataset = AbstractStorage.toVisibilityLabel(key.getColumnFamily().toString() + "_");
      Map<String, Object> json = new JsonFlattener(value.toString())
          .withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).flattenAsMap();

      // First, remove all fields that have not been explicitly asked for
      json.keySet().removeIf(field -> !acceptField(field));

      // Then, ensure the user has the right to visualize the remaining fields
      Set<String> auths = parsedAuths();

      json.keySet().removeIf(field -> {

        int index = field.indexOf(Constants.SEPARATOR_CURRENCY_SIGN);
        String vizField = AbstractStorage
            .toVisibilityLabel(vizDataset + (index <= 0 ? field : field.substring(0, index)));

        return !auths.contains(vizField);
      });

      // Next, rebuild a new JSON object
      String newJson =
          new JsonUnflattener(json).withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).unflatten();

      // Set the new JSON object as the new Accumulo Value
      if (!"{}".equals(newJson)) {
        setTopValue(new Value(newJson.getBytes(StandardCharsets.UTF_8)));
      }
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
