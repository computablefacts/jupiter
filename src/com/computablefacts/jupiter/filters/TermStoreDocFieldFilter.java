package com.computablefacts.jupiter.filters;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

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

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.nona.Generated;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class TermStoreDocFieldFilter extends Filter {

  private static final String DOCS_CRITERION = "d";
  private static final String FIELDS_CRITERION = "f";

  private BloomFilters<String> keepDocs_;
  private Set<String> keepFields_;

  public static void setDocsToKeep(IteratorSetting setting, BloomFilters<String> bf) {
    if (bf != null) {
      setting.addOption(DOCS_CRITERION, BloomFilters.toString(bf));
    }
  }

  public static void setFieldsToKeep(IteratorSetting setting, Set<String> fields) {
    if (fields != null) {
      setting.addOption(FIELDS_CRITERION, Joiner.on(SEPARATOR_NUL).join(fields));
    }
  }

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(DOCS_CRITERION, "Documents to keep.");
    options.put(FIELDS_CRITERION, "Fields patterns to keep.");

    return new IteratorOptions("TermStoreDocFieldFilter",
        "TermStoreDocFieldFilter accepts or rejects each Key/Value pair based on documents and/or fields filters.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 1 || options.size() > 2) {
      return false;
    }
    if (options.containsKey(DOCS_CRITERION)) {
      if (options.get(DOCS_CRITERION) == null) {
        return false;
      }
    }
    if (options.containsKey(FIELDS_CRITERION)) {
      return options.get(FIELDS_CRITERION) != null;
    }
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    if (!options.containsKey(DOCS_CRITERION)) {
      keepDocs_ = null;
    } else {
      keepDocs_ = BloomFilters.fromString(options.get(DOCS_CRITERION));
      if (keepDocs_ == null) {
        keepDocs_ = new BloomFilters<>();
      }
    }

    keepFields_ = options.containsKey(FIELDS_CRITERION)
        ? Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(options.get(FIELDS_CRITERION)))
        : null;
  }

  @Override
  public boolean accept(Key key, Value value) {

    if (key == null || key.getColumnQualifier() == null) {
      return false;
    }

    String cq = key.getColumnQualifier().toString();
    int index = cq.indexOf(SEPARATOR_NUL);

    String doc;
    String field;

    if (index < 0) {
      doc = null;
      field = cq;
    } else {
      doc = cq.substring(0, index);
      field = cq.substring(index + 1);
    }

    if (keepDocs_ != null && doc != null) {
      if (!acceptDoc(doc)) {
        return false;
      }
    }
    if (keepFields_ != null && field != null) {
      if (!acceptField(field)) {
        return false;
      }
    }
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

  private boolean acceptDoc(String doc) {
    return keepDocs_.mightContain(doc);
  }
}
