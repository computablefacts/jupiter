package com.computablefacts.jupiter.filters;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import com.computablefacts.asterix.Generated;
import com.computablefacts.asterix.WildcardMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

@CheckReturnValue
public class TermStoreFieldFilter extends Filter {

  private static final String FIELDS_CRITERION = "f";

  private Set<String> keepFields_;

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

    return new IteratorOptions("TermStoreFieldFilter",
        "TermStoreFieldFilter accepts or rejects each Key/Value pair based on fields filters.", options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 1 || options.size() > 2) {
      return false;
    }
    if (options.containsKey(FIELDS_CRITERION)) {
      return options.get(FIELDS_CRITERION) != null;
    }
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {

    super.init(source, options, env);

    keepFields_ = options.containsKey(FIELDS_CRITERION) ? Sets.newHashSet(
        Splitter.on(SEPARATOR_NUL).split(options.get(FIELDS_CRITERION))) : null;
  }

  @Override
  public boolean accept(Key key, Value value) {

    if (key == null || key.getColumnQualifier() == null) {
      return false;
    }

    String cq = key.getColumnQualifier().toString();
    int index = cq.indexOf(SEPARATOR_NUL);

    String field;

    if (index < 0) {
      field = cq;
    } else {
      field = cq.substring(0, index);
    }

    if (keepFields_ != null) {
      return acceptField(field);
    }
    // TODO : backport filter on term type
    return true;
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
