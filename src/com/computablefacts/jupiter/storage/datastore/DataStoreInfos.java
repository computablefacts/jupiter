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
  private final Table<String, String, Double> cardEstForBuckets_ = HashBasedTable.create();
  private final Table<String, String, Double> cardEstForTerms_ = HashBasedTable.create();
  private final Table<String, String, Set<String>> visibilityLabels_ = HashBasedTable.create();
  private final Table<String, String, String> lastUpdates_ = HashBasedTable.create();
  private final Table<String, String, Set<String>> types_ = HashBasedTable.create();

  public DataStoreInfos(String name) {
    name_ = name;
  }

  public void addCardinalityEstimationForTerms(String dataset, String field, int type,
      double estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (cardEstForTerms_.contains(dataset, field)) {
      double oldEstimate = cardEstForTerms_.get(dataset, field);
      cardEstForTerms_.remove(dataset, field);
      cardEstForTerms_.put(dataset, field, oldEstimate + estimate);
    } else {
      cardEstForTerms_.put(dataset, field, estimate);
    }

    addType(dataset, field, type);
  }

  public void addCardinalityEstimationForBuckets(String dataset, String field, int type,
      double estimate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (cardEstForBuckets_.contains(dataset, field)) {
      double oldEstimate = cardEstForBuckets_.get(dataset, field);
      cardEstForBuckets_.remove(dataset, field);
      cardEstForBuckets_.put(dataset, field, oldEstimate + estimate);
    } else {
      cardEstForBuckets_.put(dataset, field, estimate);
    }

    addType(dataset, field, type);
  }

  public void addVisibilityLabels(String dataset, String field, int type, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    if (visibilityLabels_.contains(dataset, field)) {
      visibilityLabels_.get(dataset, field).addAll(labels);
    } else {
      visibilityLabels_.put(dataset, field, new HashSet<>(labels));
    }

    addType(dataset, field, type);
  }

  public void addLastUpdate(String dataset, String field, int type, String lastUpdate) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (lastUpdates_.contains(dataset, field)) {
      String oldLastUpdate = lastUpdates_.get(dataset, field);
      int cmp = oldLastUpdate.compareTo(lastUpdate);
      if (cmp < 0) {
        lastUpdates_.remove(dataset, field);
        lastUpdates_.put(dataset, field, lastUpdate);
      }
    } else {
      lastUpdates_.put(dataset, field, lastUpdate);
    }

    addType(dataset, field, type);
  }

  public Map<String, Object> json() {

    Set<Map.Entry<String, String>> set = new HashSet<>();
    set.addAll(cardEstForBuckets_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(cardEstForTerms_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(visibilityLabels_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));
    set.addAll(lastUpdates_.cellSet().stream()
        .map(cell -> new AbstractMap.SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
        .collect(Collectors.toSet()));

    List<Map<String, Object>> fields = set.stream().map(cell -> {

      String dataset = cell.getKey();
      String field = cell.getValue();

      Map<String, Object> map = new HashMap<>();
      map.put("dataset", dataset);
      map.put("field", field.replace(SEPARATOR_CURRENCY_SIGN, '.'));
      map.put("last_update",
          lastUpdates_.contains(dataset, field) ? lastUpdates_.get(dataset, field) : null);
      map.put("nb_distinct_terms",
          cardEstForTerms_.contains(dataset, field) ? cardEstForTerms_.get(dataset, field) : 0);
      map.put("nb_distinct_buckets",
          cardEstForBuckets_.contains(dataset, field) ? cardEstForBuckets_.get(dataset, field) : 0);
      map.put("visibility_labels",
          visibilityLabels_.contains(dataset, field) ? visibilityLabels_.get(dataset, field)
              : Sets.newHashSet());
      map.put("types",
          types_.contains(dataset, field) ? types_.get(dataset, field) : Sets.newHashSet());

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

    if (types_.contains(dataset, field)) {
      types_.get(dataset, field).add(newType);
    } else {
      types_.put(dataset, field, Sets.newHashSet(newType));
    }
  }
}
