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

import com.computablefacts.asterix.Generated;
import com.computablefacts.asterix.WildcardMatcher;
import com.computablefacts.jupiter.BloomFilters;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class TermStoreBucketFieldFilter extends Filter {

  private static final String BUCKETS_CRITERION = "d";
  private static final String FIELDS_CRITERION = "f";

  private BloomFilters<String> keepBucketsIds_;
  private Set<String> keepFields_;

  public static void setDocsToKeep(IteratorSetting setting, BloomFilters<String> bucketsIds) {
    if (bucketsIds != null) {
      setting.addOption(BUCKETS_CRITERION, BloomFilters.toString(bucketsIds));
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
    options.put(BUCKETS_CRITERION, "Buckets ids to keep.");
    options.put(FIELDS_CRITERION, "Fields patterns to keep.");

    return new IteratorOptions("TermStoreBucketFieldFilter",
        "TermStoreBucketFieldFilter accepts or rejects each Key/Value pair based on buckets ids and/or fields filters.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 1 || options.size() > 2) {
      return false;
    }
    if (options.containsKey(BUCKETS_CRITERION)) {
      if (options.get(BUCKETS_CRITERION) == null) {
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

    if (!options.containsKey(BUCKETS_CRITERION)) {
      keepBucketsIds_ = null;
    } else {
      keepBucketsIds_ = BloomFilters.fromString(options.get(BUCKETS_CRITERION));
      if (keepBucketsIds_ == null) {
        keepBucketsIds_ = new BloomFilters<>();
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
    String bucketId = cq.substring(0, index);

    String field;

    int index2 = cq.lastIndexOf(SEPARATOR_NUL);
    if (index == index2) {
      field = cq.substring(index + 1);
    } else {
      field = cq.substring(index + 1, index2);
    }

    if (keepBucketsIds_ != null) {
      if (!acceptBucket(bucketId)) {
        return false;
      }
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

  private boolean acceptBucket(String bucketId) {
    return keepBucketsIds_.mightContain(bucketId);
  }
}
