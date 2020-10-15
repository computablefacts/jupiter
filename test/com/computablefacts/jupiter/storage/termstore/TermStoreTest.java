package com.computablefacts.jupiter.storage.termstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class TermStoreTest {

  @Test
  public void testCreateAndIsReady() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    Assert.assertTrue(termStore.create()); // ensure create is reentrant
    Assert.assertTrue(termStore.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());

    Assert.assertTrue(termStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(termStore.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndTruncate() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCardSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCardThird(termStore, i, auths));

      Assert.assertNotNull(fieldCountFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCountSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCountThird(termStore, i, auths));

      Assert.assertNotNull(fieldLabelsFirst(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsSecond(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(termStore, i, auths));
      Assert.assertEquals(1, countSecond(termStore, i, auths));
      Assert.assertEquals(1, countThird(termStore, i, auths));
    }

    Assert.assertTrue(termStore.truncate());

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCardFirst(termStore, i, auths));
      Assert.assertNull(fieldCardSecond(termStore, i, auths));
      Assert.assertNull(fieldCardThird(termStore, i, auths));

      Assert.assertNull(fieldCountFirst(termStore, i, auths));
      Assert.assertNull(fieldCountSecond(termStore, i, auths));
      Assert.assertNull(fieldCountThird(termStore, i, auths));

      Assert.assertNull(fieldLabelsFirst(termStore, i, auths));
      Assert.assertNull(fieldLabelsSecond(termStore, i, auths));
      Assert.assertNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(0, countFirst(termStore, i, auths));
      Assert.assertEquals(0, countSecond(termStore, i, auths));
      Assert.assertEquals(0, countThird(termStore, i, auths));
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndRemoveDataset() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCardSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCardThird(termStore, i, auths));

      Assert.assertNotNull(fieldCountFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCountSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCountThird(termStore, i, auths));

      Assert.assertNotNull(fieldLabelsFirst(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsSecond(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(termStore, i, auths));
      Assert.assertEquals(1, countSecond(termStore, i, auths));
      Assert.assertEquals(1, countThird(termStore, i, auths));
    }

    try (BatchDeleter deleter = termStore.deleter(auths)) {
      Assert.assertTrue(termStore.removeDataset(deleter, "first_dataset"));
      Assert.assertTrue(termStore.removeDataset(deleter, "second_dataset"));
    }

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCardFirst(termStore, i, auths));
      Assert.assertNull(fieldCardSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCardThird(termStore, i, auths));

      Assert.assertNull(fieldCountFirst(termStore, i, auths));
      Assert.assertNull(fieldCountSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCountThird(termStore, i, auths));

      Assert.assertNull(fieldLabelsFirst(termStore, i, auths));
      Assert.assertNull(fieldLabelsSecond(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(0, countFirst(termStore, i, auths));
      Assert.assertEquals(0, countSecond(termStore, i, auths));
      Assert.assertEquals(1, countThird(termStore, i, auths));
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testCreateAndRemoveRows() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCardSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCardThird(termStore, i, auths));

      Assert.assertNotNull(fieldCountFirst(termStore, i, auths));
      Assert.assertNotNull(fieldCountSecond(termStore, i, auths));
      Assert.assertNotNull(fieldCountThird(termStore, i, auths));

      Assert.assertNotNull(fieldLabelsFirst(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsSecond(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(termStore, i, auths));
      Assert.assertEquals(1, countSecond(termStore, i, auths));
      Assert.assertEquals(1, countThird(termStore, i, auths));
    }

    try (BatchDeleter deleter = termStore.deleter(auths)) {
      for (int i = 0; i < 100; i++) {
        if (i % 2 == 0) { // remove even rows from dataset 1
          Assert.assertTrue(termStore.removeTerm(deleter, "first_dataset", "term_" + i));
        } else { // remove odd rows from dataset 2
          Assert.assertTrue(termStore.removeTerm(deleter, "second_dataset", "term_" + i));
        }
      }
    }

    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {

        // Test metadata
        Assert.assertNotNull(fieldCardSecond(termStore, i, auths));
        Assert.assertNotNull(fieldCountSecond(termStore, i, auths));
        Assert.assertNotNull(fieldLabelsSecond(termStore, i, auths));

        // TODO : Assert.assertNull(fieldCardFirst(termStore, i, auths));
        // TODO : Assert.assertNull(fieldCountFirst(termStore, i, auths));
        // TODO : Assert.assertNull(fieldLabelsFirst(termStore, i, auths));

        // Test terms
        Assert.assertEquals(1, countSecond(termStore, i, auths));
        Assert.assertEquals(0, countFirst(termStore, i, auths));
      } else {

        // Test metadata
        Assert.assertNotNull(fieldCardFirst(termStore, i, auths));
        Assert.assertNotNull(fieldCountFirst(termStore, i, auths));
        Assert.assertNotNull(fieldLabelsFirst(termStore, i, auths));

        // TODO : Assert.assertNull(fieldCardSecond(termStore, i, auths));
        // TODO : Assert.assertNull(fieldCountSecond(termStore, i, auths));
        // TODO : Assert.assertNull(fieldLabelsSecond(termStore, i, auths));

        // Test terms
        Assert.assertEquals(1, countFirst(termStore, i, auths));
        Assert.assertEquals(0, countSecond(termStore, i, auths));
      }

      // Test metadata
      Assert.assertNotNull(fieldCardThird(termStore, i, auths));
      Assert.assertNotNull(fieldCountThird(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countThird(termStore, i, auths));
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    // OK
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, countFirst(termStore, i, authsDS1));
      Assert.assertEquals(0, countSecond(termStore, i, authsDS1));
      Assert.assertEquals(1, countThird(termStore, i, authsDS1));
    }

    // KO (throws an exception)
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(0, countFirst(termStore, i, authsDS2));
      Assert.assertEquals(1, countSecond(termStore, i, authsDS2));
      Assert.assertEquals(1, countThird(termStore, i, authsDS2));
    }
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1DS2);

    for (int i = 0; i < 10; i++) {

      Assert.assertEquals(1, countFirst(termStore, i, authsDS1DS2));
      Assert.assertEquals(1, countSecond(termStore, i, authsDS1DS2));
      Assert.assertEquals(1, countThird(termStore, i, authsDS1DS2));

      Assert.assertEquals(1, countFirst(termStore, i, authsDS1));
      Assert.assertEquals(0, countSecond(termStore, i, authsDS1));
      Assert.assertEquals(1, countThird(termStore, i, authsDS1));

      Assert.assertEquals(0, countFirst(termStore, i, authsDS2));
      Assert.assertEquals(1, countSecond(termStore, i, authsDS2));
      Assert.assertEquals(1, countThird(termStore, i, authsDS2));
    }

    Authorizations authsFirstSecondThird = new Authorizations("FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");

    Authorizations authsFirst =
        new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ");

    Authorizations authsSecond =
        new Authorizations("SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ");

    Authorizations authsThird =
        new Authorizations("THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsFirstSecondThird);

    for (int i = 0; i < 10; i++) {

      Assert.assertNotNull(fieldCardFirst(termStore, i, authsFirst));
      Assert.assertNull(fieldCardSecond(termStore, i, authsFirst));
      Assert.assertNull(fieldCardThird(termStore, i, authsFirst));

      Assert.assertNotNull(fieldCountFirst(termStore, i, authsFirst));
      Assert.assertNull(fieldCountSecond(termStore, i, authsFirst));
      Assert.assertNull(fieldCountThird(termStore, i, authsFirst));

      Assert.assertNotNull(fieldLabelsFirst(termStore, i, authsFirst));
      Assert.assertNull(fieldLabelsSecond(termStore, i, authsFirst));
      Assert.assertNull(fieldLabelsThird(termStore, i, authsFirst));

      Assert.assertNull(fieldCardFirst(termStore, i, authsSecond));
      Assert.assertNotNull(fieldCardSecond(termStore, i, authsSecond));
      Assert.assertNull(fieldCardThird(termStore, i, authsSecond));

      Assert.assertNull(fieldCountFirst(termStore, i, authsSecond));
      Assert.assertNotNull(fieldCountSecond(termStore, i, authsSecond));
      Assert.assertNull(fieldCountThird(termStore, i, authsSecond));

      Assert.assertNull(fieldLabelsFirst(termStore, i, authsSecond));
      Assert.assertNotNull(fieldLabelsSecond(termStore, i, authsSecond));
      Assert.assertNull(fieldLabelsThird(termStore, i, authsSecond));

      Assert.assertNull(fieldCardFirst(termStore, i, authsThird));
      Assert.assertNull(fieldCardSecond(termStore, i, authsThird));
      Assert.assertNotNull(fieldCardThird(termStore, i, authsThird));

      Assert.assertNull(fieldCountFirst(termStore, i, authsThird));
      Assert.assertNull(fieldCountSecond(termStore, i, authsThird));
      Assert.assertNotNull(fieldCountThird(termStore, i, authsThird));

      Assert.assertNull(fieldLabelsFirst(termStore, i, authsThird));
      Assert.assertNull(fieldLabelsSecond(termStore, i, authsThird));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, authsThird));

      Assert.assertNotNull(fieldCardFirst(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCardSecond(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCardThird(termStore, i, authsFirstSecondThird));

      Assert.assertNotNull(fieldCountFirst(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountSecond(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountThird(termStore, i, authsFirstSecondThird));

      Assert.assertNotNull(fieldLabelsFirst(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsSecond(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsThird(termStore, i, authsFirstSecondThird));
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testFieldCard() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldCard fc1 = fieldCardFirst(termStore, i, auths);
      FieldCard fc2 = fieldCardSecond(termStore, i, auths);
      FieldCard fc3 = fieldCardThird(termStore, i, auths);

      Assert.assertEquals("field_" + i, fc1.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "FIRST_DATASET_CARD"), fc1.labels());
      Assert.assertEquals(1, fc1.cardinality());

      Assert.assertEquals("field_" + i, fc2.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "SECOND_DATASET_CARD"), fc2.labels());
      Assert.assertEquals(1, fc2.cardinality());

      Assert.assertEquals("field_" + i, fc3.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "THIRD_DATASET_CARD"), fc3.labels());
      Assert.assertEquals(1, fc3.cardinality());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testFieldCount() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldCount fc1 = fieldCountFirst(termStore, i, auths);
      FieldCount fc2 = fieldCountSecond(termStore, i, auths);
      FieldCount fc3 = fieldCountThird(termStore, i, auths);

      Assert.assertEquals("field_" + i, fc1.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "FIRST_DATASET_CNT"), fc1.labels());
      Assert.assertEquals(1, fc1.count());

      Assert.assertEquals("field_" + i, fc2.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "SECOND_DATASET_CNT"), fc2.labels());
      Assert.assertEquals(1, fc2.count());

      Assert.assertEquals("field_" + i, fc3.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "THIRD_DATASET_CNT"), fc3.labels());
      Assert.assertEquals(2, fc3.count());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testFieldLabels() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldLabels fl1 = fieldLabelsFirst(termStore, i, auths);
      FieldLabels fl2 = fieldLabelsSecond(termStore, i, auths);
      FieldLabels fl3 = fieldLabelsThird(termStore, i, auths);

      Assert.assertEquals("field_" + i, fl1.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "FIRST_DATASET_VIZ"), fl1.accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_1"), fl1.termLabels());

      Assert.assertEquals("field_" + i, fl2.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "SECOND_DATASET_VIZ"), fl2.accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_2"), fl2.termLabels());

      Assert.assertEquals("field_" + i, fl3.field());
      Assert.assertEquals(Sets.newHashSet("ADM", "THIRD_DATASET_VIZ"), fl3.accumuloLabels());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), fl3.termLabels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCountExactMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCount tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCountPrefixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term_*").forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {

        Assert.assertEquals("term_" + i, list.get(i).getFirst());
        Assert.assertEquals(1, list.get(i).getSecond().size());

        TermCount tc = list.get(i).getSecond().get(0);

        Assert.assertEquals("field_" + i, tc.field());
        Assert.assertEquals("term_" + i, tc.term());
        Assert.assertEquals(2, tc.count());
        Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
      }
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCountSuffixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "*_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCount tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCountInfixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term?1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCount tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCardExactMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      termStore.termCard(scanner, "third_dataset", "term_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCard tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(1, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCardPrefixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      termStore.termCard(scanner, "third_dataset", "term_*").forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {

        Assert.assertEquals("term_" + i, list.get(i).getFirst());
        Assert.assertEquals(1, list.get(i).getSecond().size());

        TermCard tc = list.get(i).getSecond().get(0);

        Assert.assertEquals("field_" + i, tc.field());
        Assert.assertEquals("term_" + i, tc.term());
        Assert.assertEquals(1, tc.cardinality());
        Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
      }
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCardSuffixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      termStore.termCard(scanner, "third_dataset", "*_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCard tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(1, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testTermCardInfixMatch() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      termStore.termCard(scanner, "third_dataset", "term?1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      TermCard tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(1, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet("DS_2"), tc.labels());
    }

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testAddLocalityGroup() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "terms");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    fill(termStore);

    Assert.assertTrue(
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").isEmpty());
    Assert.assertTrue(termStore.addLocalityGroup("third_dataset"));
    Assert.assertEquals(6,
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").size());
    Assert.assertTrue(termStore.addLocalityGroup("third_dataset")); // ensure reentrant
    Assert.assertEquals(6,
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").size());
  }

  private void fill(TermStore termStore) throws Exception {

    Preconditions.checkNotNull(termStore, "termStore should not be null");

    @Var
    IngestStats stats = null;

    try (BatchWriter writer = termStore.writer()) {

      stats = new IngestStats(termStore, writer);

      for (int i = 0; i < 10; i++) {

        stats.card("first_dataset", "field_" + i, 1);

        Assert.assertTrue(termStore.add(writer, stats, "first_dataset", "row_" + i, "field_" + i,
            "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
            Sets.newHashSet("DS_1"), Sets.newHashSet()));
      }

      for (int i = 0; i < 10; i++) {

        stats.card("second_dataset", "field_" + i, 1);

        Assert.assertTrue(termStore.add(writer, stats, "second_dataset", "row_" + i, "field_" + i,
            "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
            Sets.newHashSet("DS_2"), Sets.newHashSet()));
      }

      for (int i = 0; i < 10; i++) {

        stats.card("third_dataset", "field_" + i, 1);

        Assert.assertTrue(
            termStore.add(writer, stats, "third_dataset", "row_" + i, "field_" + i, "term_" + i,
                Lists.newArrayList(new Pair<>(0, ("term_" + i).length()),
                    new Pair<>(10, 10 + ("term_" + i).length())),
                Sets.newHashSet("DS_1"), Sets.newHashSet("DS_2")));
      }

      stats.flush();
    }
  }

  private int countFirst(TermStore termStore, int term, Authorizations authorizations) {
    return count(termStore, "first_dataset", term, authorizations);
  }

  private int countSecond(TermStore termStore, int term, Authorizations authorizations) {
    return count(termStore, "second_dataset", term, authorizations);
  }

  private int countThird(TermStore termStore, int term, Authorizations authorizations) {
    return count(termStore, "third_dataset", term, authorizations);
  }

  private int count(TermStore termStore, String dataset, int term, Authorizations authorizations) {
    return all(termStore, dataset, "term_" + term, authorizations).size();
  }

  private List<Term> firstDataset(TermStore termStore, String term, Authorizations authorizations) {
    return all(termStore, "first_dataset", term, authorizations);
  }

  private List<Term> secondDataset(TermStore termStore, String term,
      Authorizations authorizations) {
    return all(termStore, "second_dataset", term, authorizations);
  }

  private List<Term> thirdDataset(TermStore termStore, String term, Authorizations authorizations) {
    return all(termStore, "third_dataset", term, authorizations);
  }

  private List<Term> all(TermStore termStore, String dataset, String term,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) { // keep order

      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator =
          termStore.termScan((ScannerBase) scanner, dataset, term, null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private FieldCard fieldCardFirst(TermStore termStore, int field, Authorizations authorizations) {
    return fieldCard(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldCard fieldCardSecond(TermStore termStore, int field, Authorizations authorizations) {
    return fieldCard(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldCard fieldCardThird(TermStore termStore, int field, Authorizations authorizations) {
    return fieldCard(termStore, "third_dataset", "field_" + field, authorizations);
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

  private FieldCount fieldCountFirst(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldCount(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldCount fieldCountSecond(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldCount(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldCount fieldCountThird(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldCount(termStore, "third_dataset", "field_" + field, authorizations);
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

  private FieldLabels fieldLabelsFirst(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLabels(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldLabels fieldLabelsSecond(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLabels(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldLabels fieldLabelsThird(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLabels(termStore, "third_dataset", "field_" + field, authorizations);
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
}
