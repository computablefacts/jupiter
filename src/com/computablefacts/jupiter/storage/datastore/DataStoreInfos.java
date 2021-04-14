package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.computablefacts.jupiter.storage.termstore.Term;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class DataStoreInfos {

  private final String name_;
  private final Table<String, String, Long> fieldsCounts_ = HashBasedTable.create();
  private final Table<String, String, Double> fieldsCardEstForBuckets_ = HashBasedTable.create();
  private final Table<String, String, Double> fieldsCardEstForTerms_ = HashBasedTable.create();
  private final Table<String, String, Set<String>> fieldsVisibilityLabels_ =
      HashBasedTable.create();
  private final Table<String, String, String> fieldsLastUpdates_ = HashBasedTable.create();
  private final Table<String, String, Set<String>> fieldsTypes_ = HashBasedTable.create();

  public DataStoreInfos(String name) {
    name_ = name;
  }

  public void addCount(String dataset, String field, int type, long count) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (fieldsCounts_.contains(dataset, field)) {
      long oldCount = fieldsCounts_.get(dataset, field);
      fieldsCounts_.remove(dataset, field);
      fieldsCounts_.put(dataset, field, oldCount + count);
    } else {
      fieldsCounts_.put(dataset, field, count);
    }

    addType(dataset, field, type);
  }

  public void addCardinalityEstimationForTerms(String dataset, String field, int type,
      double estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (fieldsCardEstForTerms_.contains(dataset, field)) {
      double oldEstimate = fieldsCardEstForTerms_.get(dataset, field);
      fieldsCardEstForTerms_.remove(dataset, field);
      fieldsCardEstForTerms_.put(dataset, field, oldEstimate + estimate);
    } else {
      fieldsCardEstForTerms_.put(dataset, field, estimate);
    }

    addType(dataset, field, type);
  }

  public void addCardinalityEstimationForBuckets(String dataset, String field, int type,
      double estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (fieldsCardEstForBuckets_.contains(dataset, field)) {
      double oldEstimate = fieldsCardEstForBuckets_.get(dataset, field);
      fieldsCardEstForBuckets_.remove(dataset, field);
      fieldsCardEstForBuckets_.put(dataset, field, oldEstimate + estimate);
    } else {
      fieldsCardEstForBuckets_.put(dataset, field, estimate);
    }

    addType(dataset, field, type);
  }

  public void addVisibilityLabels(String dataset, String field, int type, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    if (fieldsVisibilityLabels_.contains(dataset, field)) {
      fieldsVisibilityLabels_.get(dataset, field).addAll(labels);
    } else {
      fieldsVisibilityLabels_.put(dataset, field, new HashSet<>(labels));
    }

    addType(dataset, field, type);
  }

  public void addLastUpdate(String dataset, String field, int type, String lastUpdate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (fieldsLastUpdates_.contains(dataset, field)) {
      String oldLastUpdate = fieldsLastUpdates_.get(dataset, field);
      int cmp = oldLastUpdate.compareTo(lastUpdate);
      if (cmp < 0) {
        fieldsLastUpdates_.remove(dataset, field);
        fieldsLastUpdates_.put(dataset, field, lastUpdate);
      }
    } else {
      fieldsLastUpdates_.put(dataset, field, lastUpdate);
    }

    addType(dataset, field, type);
  }

  public Map<String, Object> json() {

    Set<Map.Entry<String, String>> set = new HashSet<>();
    set.addAll(fieldsCounts_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(fieldsCardEstForBuckets_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(fieldsCardEstForTerms_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(fieldsVisibilityLabels_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(fieldsLastUpdates_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));

    List<Map<String, Object>> fields = set.stream().map(cell -> {

      String dataset = cell.getKey();
      String field = cell.getValue();

      Map<String, Object> map = new HashMap<>();
      map.put("dataset", dataset);
      map.put("field", field.replace(SEPARATOR_CURRENCY_SIGN, '.'));
      map.put("last_update",
          fieldsLastUpdates_.contains(dataset, field) ? fieldsLastUpdates_.get(dataset, field)
              : null);
      map.put("nb_index_entries",
          fieldsCounts_.contains(dataset, field) ? fieldsCounts_.get(dataset, field) : 0);
      map.put("nb_distinct_terms",
          fieldsCardEstForTerms_.contains(dataset, field)
              ? fieldsCardEstForTerms_.get(dataset, field)
              : 0);
      map.put("nb_distinct_buckets",
          fieldsCardEstForBuckets_.contains(dataset, field)
              ? fieldsCardEstForBuckets_.get(dataset, field)
              : 0);
      map.put("visibility_labels",
          fieldsVisibilityLabels_.contains(dataset, field)
              ? fieldsVisibilityLabels_.get(dataset, field)
              : Sets.newHashSet());
      map.put("types", fieldsTypes_.contains(dataset, field) ? fieldsTypes_.get(dataset, field)
          : Sets.newHashSet());

      return map;
    }).collect(Collectors.toList());

    Map<String, Object> map = new HashMap<>();
    map.put("name", name_);
    map.put("fields", fields);

    return map;
  }

  private void addType(String dataset, String field, int type) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    String newType = type == Term.TYPE_STRING ? "TEXT"
        : type == Term.TYPE_DATE ? "DATE"
            : type == Term.TYPE_NUMBER ? "NUMBER"
                : type == Term.TYPE_BOOLEAN ? "BOOLEAN" : "UNKNOWN";

    if (fieldsTypes_.contains(dataset, field)) {
      fieldsTypes_.get(dataset, field).add(newType);
    } else {
      fieldsTypes_.put(dataset, field, Sets.newHashSet(newType));
    }
  }
}
