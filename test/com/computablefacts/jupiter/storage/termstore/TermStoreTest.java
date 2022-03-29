package com.computablefacts.jupiter.storage.termstore;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class TermStoreTest extends MiniAccumuloClusterTest {

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
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, "john").toList();

    Assert.assertEquals(1, termsCounts.size());

    @Var
    List<Term> terms = termStore.terms(auths, dataset, "john").toList();

    Assert.assertEquals(1, terms.size());

    // Remove all entries
    Assert.assertTrue(termStore.truncate());

    // Check the index is now empty
    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "john").toList();

    Assert.assertTrue(termsCounts.isEmpty());

    terms = termStore.terms(auths, dataset, "john").toList();

    Assert.assertTrue(terms.isEmpty());
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
    // Check first dataset
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, "dataset_1", "john").toList();

    Assert.assertEquals(1, termsCounts.size());

    @Var
    List<Term> terms = termStore.terms(auths, "dataset_1", "john").toList();

    Assert.assertEquals(1, terms.size());

    // Check second dataset
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, "dataset_2", "john").toList();

    Assert.assertEquals(1, termsCounts.size());

    terms = termStore.terms(auths, "dataset_2", "john").toList();

    Assert.assertEquals(1, terms.size());

    // Remove "dataset_1"
    Assert.assertTrue(termStore.removeDataset("dataset_1"));

    // Check the index has been updated
    // First dataset has been removed
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, "dataset_1", "john").toList();

    Assert.assertTrue(termsCounts.isEmpty());

    terms = termStore.terms(auths, "dataset_1", "john").toList();

    Assert.assertTrue(terms.isEmpty());

    // Second dataset is always present
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, "dataset_2", "john").toList();

    Assert.assertEquals(1, termsCounts.size());

    terms = termStore.terms(auths, "dataset_2", "john").toList();

    Assert.assertEquals(1, terms.size());
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

    // Check counts
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, "jo*").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("john", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "ja*").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("jane", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "j???").toList();

    Assert.assertEquals(2, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("jane", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());
    Assert.assertEquals("first_name", termsCounts.get(1).field());
    Assert.assertEquals("john", termsCounts.get(1).term());
    Assert.assertEquals(1, termsCounts.get(1).count());

    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "d?e").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("last_name", termsCounts.get(0).field());
    Assert.assertEquals("doe", termsCounts.get(0).term());
    Assert.assertEquals(2, termsCounts.get(0).count());

    // Check terms
    @Var
    List<Term> bucketsIds = termStore.terms(auths, dataset, "jo*").toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("1", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("john", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, "ja*").toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, "j???").toList();

    Assert.assertEquals(2, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());
    Assert.assertEquals("1", bucketsIds.get(1).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(1).field());
    Assert.assertEquals("john", bucketsIds.get(1).term());
    Assert.assertEquals(1, bucketsIds.get(1).count());

    bucketsIds = termStore.terms(auths, dataset, "d?e").toList();

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

    // Check counts
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, "*hn").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("john", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "*ne").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("jane", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    termsCounts = termStore.termCardinalityEstimationForBuckets(auths, dataset, "?oe").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("last_name", termsCounts.get(0).field());
    Assert.assertEquals("doe", termsCounts.get(0).term());
    Assert.assertEquals(2, termsCounts.get(0).count());

    // Check terms
    @Var
    List<Term> bucketsIds = termStore.terms(auths, dataset, "*hn").toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("1", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("john", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, "*ne").toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, "?oe").toList();

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

    // Throws an exception
    Iterator<TermDistinctBuckets> iterator = termStore.termCardinalityEstimationForBuckets(auths,
        dataset, Sets.newHashSet("age"), null, null);
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

    // Throws an exception
    Iterator<Term> iterator =
        termStore.terms(auths, dataset, Sets.newHashSet("age"), null, null, null);
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

    // Throws an exception
    Iterator<TermDistinctBuckets> iterator = termStore.termCardinalityEstimationForBuckets(auths,
        dataset, Sets.newHashSet("last_name"), "do*", null);
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

    // Throws an exception
    Iterator<Term> iterator =
        termStore.terms(auths, dataset, Sets.newHashSet("last_name"), "do*", null, null);
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

    // Throws an exception
    Iterator<TermDistinctBuckets> iterator =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, null, "do*", 37);
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

    // Throws an exception
    Iterator<Term> iterator = termStore.terms(auths, dataset, null, "do*", 37, null);
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

    Set<String> fields = Sets.newHashSet("age");

    // [-inf, 30]
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, null, 30).toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("age", termsCounts.get(0).field());
    Assert.assertEquals("27", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    @Var
    List<Term> bucketsIds = termStore.terms(auths, dataset, fields, null, 30, null).toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("age", bucketsIds.get(0).field());
    Assert.assertEquals("27", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    // [20, 30]
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, 20, 30).toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("age", termsCounts.get(0).field());
    Assert.assertEquals("27", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, fields, 20, 30, null).toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("age", bucketsIds.get(0).field());
    Assert.assertEquals("27", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    // [20, 40]
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, 20, 40).toList();

    Assert.assertEquals(2, termsCounts.size());
    Assert.assertEquals("age", termsCounts.get(0).field());
    Assert.assertEquals("27", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());
    Assert.assertEquals("age", termsCounts.get(1).field());
    Assert.assertEquals("37", termsCounts.get(1).term());
    Assert.assertEquals(1, termsCounts.get(1).count());

    bucketsIds = termStore.terms(auths, dataset, fields, 20, 40, null).toList();

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
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, 30, null).toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("age", termsCounts.get(0).field());
    Assert.assertEquals("37", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, fields, 30, null, null).toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("1", bucketsIds.get(0).bucketId());
    Assert.assertEquals("age", bucketsIds.get(0).field());
    Assert.assertEquals("37", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    // [30, +inf] without filter on fields
    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, null, 30, null).toList();

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

    bucketsIds = termStore.terms(auths, dataset, null, 30, null, null).toList();

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

    Set<String> fields = Sets.newHashSet("first_name");

    // Check counts
    @Var
    List<TermDistinctBuckets> termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, "ja", "jp").toList();

    Assert.assertEquals(2, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("jane", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());
    Assert.assertEquals("first_name", termsCounts.get(1).field());
    Assert.assertEquals("john", termsCounts.get(1).term());
    Assert.assertEquals(1, termsCounts.get(1).count());

    termsCounts =
        termStore.termCardinalityEstimationForBuckets(auths, dataset, fields, "ja", "jo").toList();

    Assert.assertEquals(1, termsCounts.size());
    Assert.assertEquals("first_name", termsCounts.get(0).field());
    Assert.assertEquals("jane", termsCounts.get(0).term());
    Assert.assertEquals(1, termsCounts.get(0).count());

    // Check buckets ids
    @Var
    List<Term> bucketsIds = termStore.terms(auths, dataset, fields, "ja", "jp", null).toList();

    Assert.assertEquals(2, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());
    Assert.assertEquals("1", bucketsIds.get(1).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(1).field());
    Assert.assertEquals("john", bucketsIds.get(1).term());
    Assert.assertEquals(1, bucketsIds.get(1).count());

    bucketsIds = termStore.terms(auths, dataset, fields, "ja", "jo", null).toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).count());
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

    @Var
    List<Term> bucketsIds = termStore.terms(auths, dataset, null, "jan*", bfs).toList();

    Assert.assertEquals(1, bucketsIds.size());
    Assert.assertEquals(dataset, bucketsIds.get(0).dataset());
    Assert.assertEquals("2", bucketsIds.get(0).bucketId());
    Assert.assertEquals("first_name", bucketsIds.get(0).field());
    Assert.assertEquals("jane", bucketsIds.get(0).term());
    Assert.assertEquals(1, bucketsIds.get(0).type());
    Assert.assertEquals(0, bucketsIds.get(0).labels().size());
    Assert.assertEquals(1, bucketsIds.get(0).count());

    bucketsIds = termStore.terms(auths, dataset, null, "joh*", bfs).toList();

    Assert.assertEquals(0, bucketsIds.size());
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
