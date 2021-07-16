package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.AUTH_ADM;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

public class TermStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void addLocalityGroup() throws Exception {

    String dataset = "terms";
    String bucketId = "1";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, bucketId, "age", 37, 1, labels, labels));
    }

    Map<String, Set<Text>> groupsBefore = Tables
        .getLocalityGroups(termStore.configurations().tableOperations(), termStore.tableName());

    Assert.assertEquals(0, groupsBefore.size());

    Assert.assertTrue(termStore.addLocalityGroup(dataset));

    Map<String, Set<Text>> groupsAfter = Tables
        .getLocalityGroups(termStore.configurations().tableOperations(), termStore.tableName());

    Assert.assertEquals(9, groupsAfter.size());
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_DB")),
        groupsAfter.get(dataset + "_DB"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_DT")),
        groupsAfter.get(dataset + "_DT"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_LU")),
        groupsAfter.get(dataset + "_LU"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_TT")),
        groupsAfter.get(dataset + "_TT"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_VIZ")),
        groupsAfter.get(dataset + "_VIZ"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_FCNT")),
        groupsAfter.get(dataset + "_FCNT"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_BCNT")),
        groupsAfter.get(dataset + "_BCNT"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_FIDX")),
        groupsAfter.get(dataset + "_FIDX"));
    Assert.assertEquals(Sets.newHashSet(new Text(dataset + "_BIDX")),
        groupsAfter.get(dataset + "_BIDX"));
  }

  @Test
  public void testTruncate() throws Exception {

    String dataset = "terms";
    String bucketId = "1";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      termStore.beginIngest();

      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, bucketId, "age", 37, 1, labels, labels));
      Assert.assertTrue(termStore.endIngest(writer, dataset));
    }

    // Check the index has been filled
    try (Scanner scanner = termStore.scanner(auths)) {

      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldVisibilityLabels(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "john").forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());
    }

    // Remove all entries
    Assert.assertTrue(termStore.truncate());

    // Check the index is now empty
    try (Scanner scanner = termStore.scanner(auths)) {

      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldVisibilityLabels(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertTrue(fieldLabels.isEmpty());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertTrue(fieldLastUpdates.isEmpty());

      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertTrue(termsCounts.isEmpty());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "john").forEachRemaining(terms::add);

      Assert.assertTrue(terms.isEmpty());
    }
  }

  @Test
  public void testRemoveDataset() throws Exception {

    String bucketId = "1";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      termStore.beginIngest();

      Assert.assertTrue(
          termStore.put(writer, "dataset_1", bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, "dataset_1", bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, "dataset_1", bucketId, "age", 37, 1, labels, labels));

      Assert.assertTrue(termStore.endIngest(writer, "dataset_1"));
      termStore.beginIngest();

      Assert.assertTrue(
          termStore.put(writer, "dataset_2", bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, "dataset_2", bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, "dataset_2", bucketId, "age", 37, 1, labels, labels));

      Assert.assertTrue(termStore.endIngest(writer, "dataset_2"));
    }

    // Check the index has been filled
    try (Scanner scanner = termStore.scanner(auths)) {

      // Check first dataset
      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldVisibilityLabels(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, "dataset_1", "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, "dataset_1", "john").forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());

      // Check second dataset
      fieldLabels.clear();
      termStore.fieldVisibilityLabels(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      fieldLastUpdates.clear();
      termStore.fieldLastUpdate(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, "dataset_2", "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      terms.clear();
      termStore.bucketsIds(scanner, "dataset_2", "john").forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());
    }

    // Remove "dataset_1"
    try (BatchDeleter deleter = termStore.deleter(auths)) {
      Assert.assertTrue(termStore.removeDataset(deleter, "dataset_1"));
    }

    // Check the index has been updated
    try (Scanner scanner = termStore.scanner(auths)) {

      // First dataset has been removed
      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldVisibilityLabels(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertTrue(fieldLabels.isEmpty());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertTrue(fieldLastUpdates.isEmpty());

      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, "dataset_1", "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertTrue(termsCounts.isEmpty());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, "dataset_1", "john").forEachRemaining(terms::add);

      Assert.assertTrue(terms.isEmpty());

      // Second dataset is always present
      fieldLabels.clear();
      termStore.fieldVisibilityLabels(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      fieldLastUpdates.clear();
      termStore.fieldLastUpdate(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, "dataset_2", "john")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      terms.clear();
      termStore.bucketsIds(scanner, "dataset_2", "john").forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());
    }
  }

  @Test
  public void testHitsForwardIndex() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "TERMS_");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));

      Assert
          .assertTrue(termStore.put(writer, dataset, "2", "first_name", "jane", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "age", 27, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Check counts
      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "jo*")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "jo*")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "ja*")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "ja*")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "j???")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("first_name", termsCounts.get(1).field());
      Assert.assertEquals("john", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "j???")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("first_name", termsCounts.get(1).field());
      Assert.assertEquals("john", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "d?e")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("last_name", termsCounts.get(0).field());
      Assert.assertEquals("doe", termsCounts.get(0).term());
      Assert.assertEquals(2, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "d?e")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("last_name", termsCounts.get(0).field());
      Assert.assertEquals("doe", termsCounts.get(0).term());
      Assert.assertEquals(2, termsCounts.get(0).count());

      // Check terms
      List<Term> bucketsIds = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "jo*").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("john", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "jo*").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("john", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, "ja*").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "ja*").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, "j???").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("1", bucketsIds.get(1).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(1).field());
      Assert.assertEquals("john", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "j???").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("1", bucketsIds.get(1).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(1).field());
      Assert.assertEquals("john", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, "d?e").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(0).field());
      Assert.assertEquals("doe", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("2", bucketsIds.get(1).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(1).field());
      Assert.assertEquals("doe", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "d?e").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(0).field());
      Assert.assertEquals("doe", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("2", bucketsIds.get(1).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(1).field());
      Assert.assertEquals("doe", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());
    }
  }

  @Test
  public void testHitsBackwardIndex() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));

      Assert
          .assertTrue(termStore.put(writer, dataset, "2", "first_name", "jane", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "age", 27, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Check counts
      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "*hn")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "*hn")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "*ne")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "*ne")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, "?oe")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("last_name", termsCounts.get(0).field());
      Assert.assertEquals("doe", termsCounts.get(0).term());
      Assert.assertEquals(2, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, null, "?oe")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("last_name", termsCounts.get(0).field());
      Assert.assertEquals("doe", termsCounts.get(0).term());
      Assert.assertEquals(2, termsCounts.get(0).count());

      // Check terms
      List<Term> bucketsIds = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "*hn").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("john", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "*hn").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("john", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, "*ne").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "*ne").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, "?oe").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(0).field());
      Assert.assertEquals("doe", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("2", bucketsIds.get(1).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(1).field());
      Assert.assertEquals("doe", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, null, "?oe").forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(0).field());
      Assert.assertEquals("doe", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("2", bucketsIds.get(1).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(1).field());
      Assert.assertEquals("doe", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInfiniteRangeCountsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<TermDistinctBuckets> iterator = termStore.termCardinalityEstimationForBuckets(
          scanner, dataset, Sets.newHashSet("age"), null, null);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInfiniteRangeBucketsIdsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<Term> iterator =
          termStore.bucketsIds(scanner, dataset, Sets.newHashSet("age"), null, null, null);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWildcardRangeCountsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<TermDistinctBuckets> iterator = termStore.termCardinalityEstimationForBuckets(
          scanner, dataset, Sets.newHashSet("last_name"), "do*", null);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWildcardRangeBucketsIdsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<Term> iterator =
          termStore.bucketsIds(scanner, dataset, Sets.newHashSet("last_name"), "do*", null, null);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeMismatchRangeCountsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<TermDistinctBuckets> iterator =
          termStore.termCardinalityEstimationForBuckets(scanner, dataset, null, "do*", 37);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeMismatchRangeBucketsIdsQuery() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {
      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      // Throws an exception
      Iterator<Term> iterator = termStore.bucketsIds(scanner, dataset, null, "do*", 37, null);
    }
  }

  @Test
  public void testNumericRangeQueries() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));

      Assert
          .assertTrue(termStore.put(writer, dataset, "2", "first_name", "jane", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "age", 27, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      Set<String> fields = Sets.newHashSet("age");

      // [-inf, 30]
      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, null, 30)
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("age", termsCounts.get(0).field());
      Assert.assertEquals("27", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      List<Term> bucketsIds = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, fields, null, 30, null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("age", bucketsIds.get(0).field());
      Assert.assertEquals("27", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      // [20, 30]
      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, 20, 30)
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("age", termsCounts.get(0).field());
      Assert.assertEquals("27", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, fields, 20, 30, null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("age", bucketsIds.get(0).field());
      Assert.assertEquals("27", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      // [20, 40]
      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, 20, 40)
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("age", termsCounts.get(0).field());
      Assert.assertEquals("27", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("age", termsCounts.get(1).field());
      Assert.assertEquals("37", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, fields, 20, 40, null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("age", bucketsIds.get(0).field());
      Assert.assertEquals("27", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("1", bucketsIds.get(1).bucketId());
      Assert.assertEquals("age", bucketsIds.get(1).field());
      Assert.assertEquals("37", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      // [30, +inf]
      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, 30, null)
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("age", termsCounts.get(0).field());
      Assert.assertEquals("37", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, fields, 30, null, null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("age", bucketsIds.get(0).field());
      Assert.assertEquals("37", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      // [30, +inf] without filter on fields
      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, null, 30, null)
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(4, termsCounts.size());

      Assert.assertEquals("age", termsCounts.get(0).field());
      Assert.assertEquals("37", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      Assert.assertEquals("last_name", termsCounts.get(1).field());
      Assert.assertEquals("doe", termsCounts.get(1).term());
      Assert.assertEquals(2, termsCounts.get(1).count());

      Assert.assertEquals("first_name", termsCounts.get(2).field());
      Assert.assertEquals("jane", termsCounts.get(2).term());
      Assert.assertEquals(1, termsCounts.get(2).count());

      Assert.assertEquals("first_name", termsCounts.get(3).field());
      Assert.assertEquals("john", termsCounts.get(3).term());
      Assert.assertEquals(1, termsCounts.get(3).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, null, 30, null, null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(5, bucketsIds.size());

      Assert.assertEquals("1", bucketsIds.get(0).bucketId());
      Assert.assertEquals("age", bucketsIds.get(0).field());
      Assert.assertEquals("37", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      Assert.assertEquals("1", bucketsIds.get(1).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(1).field());
      Assert.assertEquals("doe", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      Assert.assertEquals("2", bucketsIds.get(2).bucketId());
      Assert.assertEquals("last_name", bucketsIds.get(2).field());
      Assert.assertEquals("doe", bucketsIds.get(2).term());
      Assert.assertEquals(1, bucketsIds.get(2).count());

      Assert.assertEquals("2", bucketsIds.get(3).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(3).field());
      Assert.assertEquals("jane", bucketsIds.get(3).term());
      Assert.assertEquals(1, bucketsIds.get(3).count());

      Assert.assertEquals("1", bucketsIds.get(4).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(4).field());
      Assert.assertEquals("john", bucketsIds.get(4).term());
      Assert.assertEquals(1, bucketsIds.get(4).count());
    }
  }

  @Test
  public void testTextRangeQueries() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));

      Assert
          .assertTrue(termStore.put(writer, dataset, "2", "first_name", "jane", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "age", 27, 1, labels, labels));
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      Set<String> fields = Sets.newHashSet("first_name");

      // Check counts
      List<TermDistinctBuckets> termsCounts = new ArrayList<>();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, "ja", "jp")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("first_name", termsCounts.get(1).field());
      Assert.assertEquals("john", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      termsCounts.clear();
      termStore.termCardinalityEstimationForBuckets(scanner, dataset, fields, "ja", "jo")
          .forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      // Check buckets ids
      List<Term> bucketsIds = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, fields, "ja", "jp", null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(2, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
      Assert.assertEquals("1", bucketsIds.get(1).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(1).field());
      Assert.assertEquals("john", bucketsIds.get(1).term());
      Assert.assertEquals(1, bucketsIds.get(1).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, fields, "ja", "jo", null)
          .forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).count());
    }
  }

  @Test
  public void testSketches() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    TermStore termStore = newTermStore(AUTH_ADM);

    try (BatchWriter writer = termStore.writer()) {

      termStore.beginIngest();

      for (int i = 0; i < 5; i++) {

        String bucketId = "a" + i;

        Assert.assertTrue(
            termStore.put(writer, dataset, bucketId, "first_name", "john", 1, labels, labels));
        Assert.assertTrue(
            termStore.put(writer, dataset, bucketId, "last_name", "doe", 1, labels, labels));
        Assert.assertTrue(termStore.put(writer, dataset, bucketId, "age", 37, 1, labels, labels));
      }

      Assert.assertTrue(termStore.endIngest(writer, dataset));
    }

    try (Scanner scanner = termStore.scanner(AUTH_ADM)) {

      // Check distinct terms
      List<FieldDistinctTerms> distinctTerms = new ArrayList<>();
      termStore.fieldCardinalityEstimationForTerms(scanner, dataset, null)
          .forEachRemaining(distinctTerms::add);

      Assert.assertEquals(3, distinctTerms.size());

      Assert.assertEquals(dataset, distinctTerms.get(0).dataset());
      Assert.assertEquals("age", distinctTerms.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, distinctTerms.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(0).labels());
      Assert.assertEquals(1.0, distinctTerms.get(0).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctTerms.get(1).dataset());
      Assert.assertEquals("first_name", distinctTerms.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctTerms.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(1).labels());
      Assert.assertEquals(1.0, distinctTerms.get(1).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctTerms.get(2).dataset());
      Assert.assertEquals("last_name", distinctTerms.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctTerms.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(2).labels());
      Assert.assertEquals(1.0, distinctTerms.get(2).estimate(), 0.0000001);

      // Check distinct buckets
      List<FieldDistinctBuckets> distinctBuckets = new ArrayList<>();
      termStore.fieldCardinalityEstimationForBuckets(scanner, dataset, null)
          .forEachRemaining(distinctBuckets::add);

      Assert.assertEquals(3, distinctBuckets.size());

      Assert.assertEquals(dataset, distinctBuckets.get(0).dataset());
      Assert.assertEquals("age", distinctBuckets.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, distinctBuckets.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(0).labels());
      Assert.assertEquals(5.0, distinctBuckets.get(0).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctBuckets.get(1).dataset());
      Assert.assertEquals("first_name", distinctBuckets.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctBuckets.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(1).labels());
      Assert.assertEquals(5.0, distinctBuckets.get(1).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctBuckets.get(2).dataset());
      Assert.assertEquals("last_name", distinctBuckets.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctBuckets.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(2).labels());
      Assert.assertEquals(5.0, distinctBuckets.get(2).estimate(), 0.0000001);

      // Test top terms
      List<FieldTopTerms> topTerms = new ArrayList<>();
      termStore.fieldTopTerms(scanner, dataset, null).forEachRemaining(topTerms::add);

      Assert.assertEquals(3, topTerms.size());

      Multiset<String> multiset = HashMultiset.create();
      multiset.add("37", 5);

      Assert.assertEquals(dataset, topTerms.get(0).dataset());
      Assert.assertEquals("age", topTerms.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, topTerms.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(0).labels());
      Assert.assertEquals(multiset, topTerms.get(0).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(0).topTermsNoFalseNegatives());

      multiset.clear();
      multiset.add("john", 5);

      Assert.assertEquals(dataset, topTerms.get(1).dataset());
      Assert.assertEquals("first_name", topTerms.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, topTerms.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(1).labels());
      Assert.assertEquals(multiset, topTerms.get(1).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(1).topTermsNoFalseNegatives());

      multiset.clear();
      multiset.add("doe", 5);

      Assert.assertEquals(dataset, topTerms.get(2).dataset());
      Assert.assertEquals("last_name", topTerms.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, topTerms.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(2).labels());
      Assert.assertEquals(multiset, topTerms.get(2).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(2).topTermsNoFalseNegatives());
    }

    // Add more entries to the store and ensure distinct counts and top terms are updated
    try (BatchWriter writer = termStore.writer()) {

      termStore.beginIngest();

      for (int i = 0; i < 5; i++) {

        String bucketId = "b" + i;

        Assert.assertTrue(
            termStore.put(writer, dataset, bucketId, "first_name", "jane", 1, labels, labels));
        Assert.assertTrue(
            termStore.put(writer, dataset, bucketId, "last_name", "doe", 1, labels, labels));
        Assert.assertTrue(termStore.put(writer, dataset, bucketId, "age", 27, 1, labels, labels));
      }

      Assert.assertTrue(termStore.endIngest(writer, dataset));
    }

    try (Scanner scanner = termStore.scanner(AUTH_ADM)) {

      // Check distinct terms
      List<FieldDistinctTerms> distinctTerms = new ArrayList<>();
      termStore.fieldCardinalityEstimationForTerms(scanner, dataset, null)
          .forEachRemaining(distinctTerms::add);

      Assert.assertEquals(3, distinctTerms.size());

      Assert.assertEquals(dataset, distinctTerms.get(0).dataset());
      Assert.assertEquals("age", distinctTerms.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, distinctTerms.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(0).labels());
      Assert.assertEquals(2.0, distinctTerms.get(0).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctTerms.get(1).dataset());
      Assert.assertEquals("first_name", distinctTerms.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctTerms.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(1).labels());
      Assert.assertEquals(2.0, distinctTerms.get(1).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctTerms.get(2).dataset());
      Assert.assertEquals("last_name", distinctTerms.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctTerms.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DT"), distinctTerms.get(2).labels());
      Assert.assertEquals(1.0, distinctTerms.get(2).estimate(), 0.0000001);

      // Check distinct buckets
      List<FieldDistinctBuckets> distinctBuckets = new ArrayList<>();
      termStore.fieldCardinalityEstimationForBuckets(scanner, dataset, null)
          .forEachRemaining(distinctBuckets::add);

      Assert.assertEquals(3, distinctBuckets.size());

      Assert.assertEquals(dataset, distinctBuckets.get(0).dataset());
      Assert.assertEquals("age", distinctBuckets.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, distinctBuckets.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(0).labels());
      Assert.assertEquals(10.0, distinctBuckets.get(0).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctBuckets.get(1).dataset());
      Assert.assertEquals("first_name", distinctBuckets.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctBuckets.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(1).labels());
      Assert.assertEquals(10.0, distinctBuckets.get(1).estimate(), 0.0000001);

      Assert.assertEquals(dataset, distinctBuckets.get(2).dataset());
      Assert.assertEquals("last_name", distinctBuckets.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, distinctBuckets.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_DB"), distinctBuckets.get(2).labels());
      Assert.assertEquals(10.0, distinctBuckets.get(2).estimate(), 0.0000001);

      // Check top terms
      List<FieldTopTerms> topTerms = new ArrayList<>();
      termStore.fieldTopTerms(scanner, dataset, null).forEachRemaining(topTerms::add);

      Assert.assertEquals(3, topTerms.size());

      Multiset<String> multiset = HashMultiset.create();
      multiset.add("37", 5);
      multiset.add("27", 5);

      Assert.assertEquals(dataset, topTerms.get(0).dataset());
      Assert.assertEquals("age", topTerms.get(0).field());
      Assert.assertEquals(Term.TYPE_NUMBER, topTerms.get(0).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(0).labels());
      Assert.assertEquals(multiset, topTerms.get(0).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(0).topTermsNoFalseNegatives());

      multiset.clear();
      multiset.add("john", 5);
      multiset.add("jane", 5);

      Assert.assertEquals(dataset, topTerms.get(1).dataset());
      Assert.assertEquals("first_name", topTerms.get(1).field());
      Assert.assertEquals(Term.TYPE_STRING, topTerms.get(1).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(1).labels());
      Assert.assertEquals(multiset, topTerms.get(1).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(1).topTermsNoFalseNegatives());

      multiset.clear();
      multiset.add("doe", 10);

      Assert.assertEquals(dataset, topTerms.get(2).dataset());
      Assert.assertEquals("last_name", topTerms.get(2).field());
      Assert.assertEquals(Term.TYPE_STRING, topTerms.get(2).type());
      Assert.assertEquals(Sets.newHashSet("ADM", "TERMS_TT"), topTerms.get(2).labels());
      Assert.assertEquals(multiset, topTerms.get(2).topTermsNoFalsePositives());
      Assert.assertEquals(multiset, topTerms.get(2).topTermsNoFalseNegatives());
    }
  }

  @Test
  public void testQueryWithBucketsIds() throws Exception {

    String dataset = "terms";
    Set<String> labels = Sets.newHashSet();
    Authorizations auths = new Authorizations("ADM", "TERMS_FIRST_NAME");
    TermStore termStore = newTermStore(auths);

    try (BatchWriter writer = termStore.writer()) {

      Assert
          .assertTrue(termStore.put(writer, dataset, "1", "first_name", "john", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "1", "age", 37, 1, labels, labels));

      Assert
          .assertTrue(termStore.put(writer, dataset, "2", "first_name", "jane", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, "2", "age", 27, 1, labels, labels));
    }

    BloomFilters<String> bfs = new BloomFilters<>();

    for (String docId : Lists.newArrayList("2")) {
      bfs.put(docId);
    }

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> bucketsIds = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, null, "jan*", bfs).forEachRemaining(bucketsIds::add);

      Assert.assertEquals(1, bucketsIds.size());
      Assert.assertEquals(dataset, bucketsIds.get(0).dataset());
      Assert.assertEquals("2", bucketsIds.get(0).bucketId());
      Assert.assertEquals("first_name", bucketsIds.get(0).field());
      Assert.assertEquals("jane", bucketsIds.get(0).term());
      Assert.assertEquals(1, bucketsIds.get(0).type());
      Assert.assertEquals(0, bucketsIds.get(0).labels().size());
      Assert.assertEquals(1, bucketsIds.get(0).count());

      bucketsIds.clear();
      termStore.bucketsIds(scanner, dataset, null, "joh*", bfs).forEachRemaining(bucketsIds::add);

      Assert.assertEquals(0, bucketsIds.size());
    }
  }

  private TermStore newTermStore(Authorizations auths) throws Exception {

    String username = nextUsername();
    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    TermStore termStore = new TermStore(configurations, tableName);

    Assert.assertTrue(termStore.create());

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return termStore;
  }
}
