package com.computablefacts.jupiter.storage.termstore;

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

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
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

    Assert.assertEquals(4, groupsAfter.size());
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
      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, dataset, bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, dataset, bucketId, "age", 37, 1, labels, labels));
    }

    // Check the index has been filled
    try (Scanner scanner = termStore.scanner(auths)) {

      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldLabels(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      List<FieldCount> fieldCounts = new ArrayList<>();
      termStore.fieldCount(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertEquals(1, fieldCounts.size());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, "john").forEachRemaining(termsCounts::add);

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
      termStore.fieldLabels(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertTrue(fieldLabels.isEmpty());

      List<FieldCount> fieldCounts = new ArrayList<>();
      termStore.fieldCount(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertTrue(fieldCounts.isEmpty());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, dataset, Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertTrue(fieldLastUpdates.isEmpty());

      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, "john").forEachRemaining(termsCounts::add);

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

      Assert.assertTrue(
          termStore.put(writer, "dataset_1", bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, "dataset_1", bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, "dataset_1", bucketId, "age", 37, 1, labels, labels));

      Assert.assertTrue(
          termStore.put(writer, "dataset_2", bucketId, "first_name", "john", 1, labels, labels));
      Assert.assertTrue(
          termStore.put(writer, "dataset_2", bucketId, "last_name", "doe", 1, labels, labels));
      Assert.assertTrue(termStore.put(writer, "dataset_2", bucketId, "age", 37, 1, labels, labels));
    }

    // Check the index has been filled
    try (Scanner scanner = termStore.scanner(auths)) {

      // Check first dataset
      List<FieldLabels> fieldLabels = new ArrayList<>();
      termStore.fieldLabels(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      List<FieldCount> fieldCounts = new ArrayList<>();
      termStore.fieldCount(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertEquals(1, fieldCounts.size());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, "dataset_1", "john").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, "dataset_1", "john").forEachRemaining(terms::add);

      Assert.assertEquals(1, terms.size());

      // Check second dataset
      fieldLabels.clear();
      termStore.fieldLabels(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      fieldCounts.clear();
      termStore.fieldCount(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertEquals(1, fieldCounts.size());

      fieldLastUpdates.clear();
      termStore.fieldLastUpdate(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      termsCounts.clear();
      termStore.counts(scanner, "dataset_2", "john").forEachRemaining(termsCounts::add);

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
      termStore.fieldLabels(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertTrue(fieldLabels.isEmpty());

      List<FieldCount> fieldCounts = new ArrayList<>();
      termStore.fieldCount(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertTrue(fieldCounts.isEmpty());

      List<FieldLastUpdate> fieldLastUpdates = new ArrayList<>();
      termStore.fieldLastUpdate(scanner, "dataset_1", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertTrue(fieldLastUpdates.isEmpty());

      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, "dataset_1", "john").forEachRemaining(termsCounts::add);

      Assert.assertTrue(termsCounts.isEmpty());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, "dataset_1", "john").forEachRemaining(terms::add);

      Assert.assertTrue(terms.isEmpty());

      // Second dataset is always present
      fieldLabels.clear();
      termStore.fieldLabels(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLabels::add);

      Assert.assertEquals(1, fieldLabels.size());

      fieldCounts.clear();
      termStore.fieldCount(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldCounts::add);

      Assert.assertEquals(1, fieldCounts.size());

      fieldLastUpdates.clear();
      termStore.fieldLastUpdate(scanner, "dataset_2", Sets.newHashSet("first_name"))
          .forEachRemaining(fieldLastUpdates::add);

      Assert.assertEquals(1, fieldLastUpdates.size());

      termsCounts.clear();
      termStore.counts(scanner, "dataset_2", "john").forEachRemaining(termsCounts::add);

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
      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, "jo*").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, "ja*").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, "j???").forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("first_name", termsCounts.get(1).field());
      Assert.assertEquals("john", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, "d?e").forEachRemaining(termsCounts::add);

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
      termStore.bucketsIds(scanner, dataset, "ja*").forEachRemaining(bucketsIds::add);

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
      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, "*hn").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("john", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, "*ne").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, "?oe").forEachRemaining(termsCounts::add);

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
      termStore.bucketsIds(scanner, dataset, "*ne").forEachRemaining(bucketsIds::add);

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
      Iterator<TermCount> iterator =
          termStore.counts(scanner, dataset, Sets.newHashSet("age"), null, null);
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
      Iterator<TermCount> iterator =
          termStore.counts(scanner, dataset, Sets.newHashSet("last_name"), "do*", null);
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
      Iterator<TermCount> iterator = termStore.counts(scanner, dataset, null, "do*", 37);
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
      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, fields, null, 30).forEachRemaining(termsCounts::add);

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
      termStore.counts(scanner, dataset, fields, 20, 30).forEachRemaining(termsCounts::add);

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
      termStore.counts(scanner, dataset, fields, 20, 40).forEachRemaining(termsCounts::add);

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
      termStore.counts(scanner, dataset, fields, 30, null).forEachRemaining(termsCounts::add);

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
      termStore.counts(scanner, dataset, null, 30, null).forEachRemaining(termsCounts::add);

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
      List<TermCount> termsCounts = new ArrayList<>();
      termStore.counts(scanner, dataset, fields, "ja", "jp").forEachRemaining(termsCounts::add);

      Assert.assertEquals(2, termsCounts.size());
      Assert.assertEquals("first_name", termsCounts.get(0).field());
      Assert.assertEquals("jane", termsCounts.get(0).term());
      Assert.assertEquals(1, termsCounts.get(0).count());
      Assert.assertEquals("first_name", termsCounts.get(1).field());
      Assert.assertEquals("john", termsCounts.get(1).term());
      Assert.assertEquals(1, termsCounts.get(1).count());

      termsCounts.clear();
      termStore.counts(scanner, dataset, fields, "ja", "jo").forEachRemaining(termsCounts::add);

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
