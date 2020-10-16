package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.nona.Generated;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BloomFilterFilter extends Filter {

  private static final String COLUMN_CRITERION = "col";
  private static final String BF_CRITERION = "cq";

  private String column_;
  private BloomFilters<String> bloomFilters_;

  public static void applyOnRow(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "ROW");
  }

  public static void applyOnColumnFamily(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "CF");
  }

  public static void applyOnColumnQualifier(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "CQ");
  }

  public static void applyOnValue(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "VALUE");
  }

  public static void setBloomFilter(IteratorSetting setting, BloomFilters<String> bloomFilters) {
    if (bloomFilters != null) {
      setting.addOption(BF_CRITERION, BloomFilters.toString(bloomFilters));
    }
  }

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(COLUMN_CRITERION, "Column on which the filter must be applied.");
    options.put(BF_CRITERION, "Bloom filter.");

    return new IteratorOptions("BloomFilterFilter",
        "BloomFilterFilter accepts or rejects each Key/Value pair based on a given column evaluated against a given Bloom filter.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return options.size() == 2 && options.containsKey(COLUMN_CRITERION)
        && options.containsKey(BF_CRITERION) && options.get(BF_CRITERION) != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    bloomFilters_ = BloomFilters.fromString(options.get(BF_CRITERION));
    column_ = options.getOrDefault(COLUMN_CRITERION, "VALUE");
  }

  @Override
  public boolean accept(Key key, Value value) {

    if (bloomFilters_ == null) {
      return false; // no Bloom filter ? reject all key/value pairs
    }

    String val;

    if ("ROW".equals(column_)) {
      val = key == null || key.getRow() == null ? null : key.getRow().toString();
    } else if ("CF".equals(column_)) {
      val = key == null || key.getColumnFamily() == null ? null : key.getColumnFamily().toString();
    } else if ("CQ".equals(column_)) {
      val = key == null || key.getColumnQualifier() == null ? null
          : key.getColumnQualifier().toString();
    } else { // VALUE
      val = value == null ? null : value.toString();
    }
    return val == null || bloomFilters_.mightContain(val);
  }
}
