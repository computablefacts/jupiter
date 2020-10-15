package com.computablefacts.jupiter.storage.termstore;

import java.util.Iterator;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class TermStoreTest {

  @Test
  public void testCreateThenDestroyTermStore() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());
    Assert.assertEquals("terms", termStore.tableName());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateFillAndTruncate() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    MiniAccumuloClusterUtils.setUserAuths(accumulo,
        new Authorizations("DS_1", "DS_2", "DS_1", "DS_2", "FIRST_DATASET_CNT",
            "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
            "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ"));

    for (int i = 0; i < 100; i++) {

      // test fields
      Assert.assertEquals(1, fieldCard(termStore, "first_dataset", "field_" + i,
          new Authorizations("FIRST_DATASET_CARD")).cardinality());
      Assert.assertEquals(1, fieldCount(termStore, "first_dataset", "field_" + i,
          new Authorizations("FIRST_DATASET_CNT")).count());
      Assert.assertEquals(Sets.newHashSet("ADM", "FIRST_DATASET_VIZ"), fieldLabels(termStore,
          "first_dataset", "field_" + i, new Authorizations("FIRST_DATASET_VIZ")).accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_1"), fieldLabels(termStore, "first_dataset",
          "field_" + i, new Authorizations("FIRST_DATASET_VIZ")).termLabels());

      Assert.assertEquals(1, fieldCard(termStore, "second_dataset", "field_" + i,
          new Authorizations("SECOND_DATASET_CARD")).cardinality());
      Assert.assertEquals(1, fieldCount(termStore, "second_dataset", "field_" + i,
          new Authorizations("SECOND_DATASET_CNT")).count());
      Assert.assertEquals(Sets.newHashSet("ADM", "SECOND_DATASET_VIZ"),
          fieldLabels(termStore, "second_dataset", "field_" + i,
              new Authorizations("SECOND_DATASET_VIZ")).accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_2"), fieldLabels(termStore, "second_dataset",
          "field_" + i, new Authorizations("SECOND_DATASET_VIZ")).termLabels());

      Assert.assertEquals(1, fieldCard(termStore, "third_dataset", "field_" + i,
          new Authorizations("THIRD_DATASET_CARD")).cardinality());
      Assert.assertEquals(1, fieldCount(termStore, "third_dataset", "field_" + i,
          new Authorizations("THIRD_DATASET_CNT")).count());
      Assert.assertEquals(Sets.newHashSet("ADM", "THIRD_DATASET_VIZ"), fieldLabels(termStore,
          "third_dataset", "field_" + i, new Authorizations("THIRD_DATASET_VIZ")).accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), fieldLabels(termStore, "third_dataset",
          "field_" + i, new Authorizations("THIRD_DATASET_VIZ")).termLabels());

      // test terms
      Assert.assertEquals(1,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_1")));
      Assert.assertEquals(0,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_1")));
      Assert.assertEquals(1,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_1")));

      Assert.assertEquals(0,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_2")));
      Assert.assertEquals(1,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_2")));
      Assert.assertEquals(1,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_2")));

      Assert.assertEquals(1,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
      Assert.assertEquals(1,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
      Assert.assertEquals(1,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
    }

    Assert.assertTrue(termStore.truncate());

    for (int i = 0; i < 100; i++) {

      // test fields
      Assert.assertNull(fieldCard(termStore, "first_dataset", "field_" + i,
          new Authorizations("FIRST_DATASET_CARD")));
      Assert.assertNull(fieldCount(termStore, "first_dataset", "field_" + i,
          new Authorizations("FIRST_DATASET_CNT")));
      Assert.assertNull(fieldLabels(termStore, "first_dataset", "field_" + i,
          new Authorizations("FIRST_DATASET_VIZ")));

      Assert.assertNull(fieldCard(termStore, "second_dataset", "field_" + i,
          new Authorizations("SECOND_DATASET_CARD")));
      Assert.assertNull(fieldCount(termStore, "second_dataset", "field_" + i,
          new Authorizations("SECOND_DATASET_CNT")));
      Assert.assertNull(fieldLabels(termStore, "second_dataset", "field_" + i,
          new Authorizations("SECOND_DATASET_VIZ")));

      Assert.assertNull(fieldCard(termStore, "third_dataset", "field_" + i,
          new Authorizations("THIRD_DATASET_CARD")));
      Assert.assertNull(fieldCount(termStore, "third_dataset", "field_" + i,
          new Authorizations("THIRD_DATASET_CNT")));
      Assert.assertNull(fieldLabels(termStore, "third_dataset", "field_" + i,
          new Authorizations("THIRD_DATASET_VIZ")));

      // test terms
      Assert.assertEquals(0,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_1")));
      Assert.assertEquals(0,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_1")));
      Assert.assertEquals(0,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_1")));

      Assert.assertEquals(0,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_2")));
      Assert.assertEquals(0,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_2")));
      Assert.assertEquals(0,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_2")));

      Assert.assertEquals(0,
          count(termStore, "first_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
      Assert.assertEquals(0,
          count(termStore, "second_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
      Assert.assertEquals(0,
          count(termStore, "third_dataset", "term_" + i, new Authorizations("DS_1", "DS_2")));
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private void fill(TermStore termStore) throws Exception {

    Preconditions.checkNotNull(termStore, "termStore should not be null");

    @Var
    IngestStats stats = null;

    try (BatchWriter writer = termStore.writer()) {

      stats = new IngestStats(termStore, writer);

      for (int i = 0; i < 100; i++) {

        stats.card("first_dataset", "field_" + i, 1);

        boolean isOk = termStore.add(writer, stats, "first_dataset", "row_" + i, "field_" + i,
            "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
            Sets.newHashSet("DS_1"), Sets.newHashSet());
      }

      for (int i = 0; i < 100; i++) {

        stats.card("second_dataset", "field_" + i, 1);

        boolean isOk = termStore.add(writer, stats, "second_dataset", "row_" + i, "field_" + i,
            "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
            Sets.newHashSet("DS_2"), Sets.newHashSet());
      }

      for (int i = 0; i < 100; i++) {

        stats.card("third_dataset", "field_" + i, 1);

        boolean isOk = termStore.add(writer, stats, "third_dataset", "row_" + i, "field_" + i,
            "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
            Sets.newHashSet("DS_1"), Sets.newHashSet("DS_2"));
      }

      stats.flush();
    }
  }

  private FieldCard fieldCard(TermStore termStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) {
      Iterator<FieldCard> iterator = termStore.fieldCard(scanner, dataset, Sets.newHashSet(field));
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private FieldCount fieldCount(TermStore termStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) {
      Iterator<FieldCount> iterator =
          termStore.fieldCount(scanner, dataset, Sets.newHashSet(field));
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private FieldLabels fieldLabels(TermStore termStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) {
      Iterator<FieldLabels> iterator =
          termStore.fieldLabels(scanner, dataset, Sets.newHashSet(field));
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private int count(TermStore termStore, String dataset, String term,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    try (BatchScanner scanner = termStore.batchScanner(authorizations)) {
      Iterator<Term> iterator = termStore.termScan(scanner, dataset, term, null, null);
      return iterator.hasNext() ? Iterators.size(iterator) : 0;
    }
  }
}
