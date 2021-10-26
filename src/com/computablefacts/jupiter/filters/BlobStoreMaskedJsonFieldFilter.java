package com.computablefacts.jupiter.filters;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreMaskedJsonFieldFilter extends Filter {

  private static final String FILTER_CRITERION = "f";

  private final Set<Map.Entry<String, String>> filters_ = new HashSet<>();

  public static void addFilter(IteratorSetting setting, String key, String value) {
    if (!Strings.isNullOrEmpty(key)) {
      String hash = value != null && value.startsWith("MASKED_") ? value
          : "MASKED_" + MaskingIterator.hash(null, value);
      setting.addOption(FILTER_CRITERION + setting.getOptions().size(), key + SEPARATOR_NUL + hash);
    }
  }

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(FILTER_CRITERION, "Literal expressions to match.");

    return new IteratorOptions("BlobStoreMaskedJsonFieldFilter",
        "BlobStoreMaskedJsonFieldFilter accepts or rejects each Key/Value pair based on a list of JSON fields filters.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 1) {
      return false;
    }
    for (String option : options.keySet()) {
      if (option.startsWith(FILTER_CRITERION) && options.get(option) == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    options.keySet().stream().filter(option -> option.startsWith(FILTER_CRITERION))
        .map(options::get).distinct()
        .forEach(filter -> filters_
            .add(new AbstractMap.SimpleEntry<>(filter.substring(0, filter.indexOf(SEPARATOR_NUL)),
                filter.substring(filter.indexOf(SEPARATOR_NUL) + 1))));
  }

  @Override
  public boolean accept(Key key, Value value) {

    if (key == null || value == null) {
      return false;
    }

    String cf = key.getColumnFamily().toString();

    if (!BlobStore.TYPE_JSON.equals(cf)) {
      return false;
    }

    Map<String, Object> json =
        new JsonFlattener(value.toString()).withSeparator(SEPARATOR_CURRENCY_SIGN).flattenAsMap();

    for (Map.Entry<String, String> filter : filters_) {

      String field = filter.getKey();
      String hash = filter.getValue();

      Set<String> val;

      if (json.containsKey(field)) {
        val = Sets.newHashSet(json.getOrDefault(field, "").toString());
      } else {

        val = json.entrySet().stream().filter(e -> WildcardMatcher.match(e.getKey(), field))
            .map(e -> e.getValue() == null ? "" : e.getValue().toString())
            .collect(Collectors.toSet());

        if (val.isEmpty()) {
          return false;
        }
      }

      if (val.stream().noneMatch(v -> {
        if (v.startsWith("MASKED_")) {
          return v.equals(hash);
        } else {

          String hashedVal = "MASKED_" + MaskingIterator.hash(null, v);

          return hashedVal.equals(hash);
        }
      })) {
        return false;
      }
    }
    return true;
  }
}
