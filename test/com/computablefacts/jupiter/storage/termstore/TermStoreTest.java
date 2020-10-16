package com.computablefacts.jupiter.storage.termstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

/**
 * This class is not thread-safe because {@link MiniAccumuloClusterUtils#setUserAuths} is used. Do
 * not execute methods in parallel.
 */
@net.jcip.annotations.NotThreadSafe
public class TermStoreTest {

  private static MiniAccumuloCluster accumulo;
  private static Configurations configurations;
  private static TermStore termStore;

  @BeforeClass
  public static void initClass() throws Exception {
    accumulo = MiniAccumuloClusterUtils.newCluster();
    configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    termStore = new TermStore(configurations, "terms");
  }

  @AfterClass
  public static void uinitClass() throws Exception {
    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private static void fill() throws Exception {

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

  private static int countFirst(int term, Authorizations authorizations) {
    return count("first_dataset", term, authorizations);
  }

  private static int countSecond(int term, Authorizations authorizations) {
    return count("second_dataset", term, authorizations);
  }

  private static int countThird(int term, Authorizations authorizations) {
    return count("third_dataset", term, authorizations);
  }

  private static int count(String dataset, int term, Authorizations authorizations) {
    return all(dataset, "term_" + term, authorizations).size();
  }

  private static List<Term> firstDataset(String term, Authorizations authorizations) {
    return all("first_dataset", term, authorizations);
  }

  private static List<Term> secondDataset(String term, Authorizations authorizations) {
    return all("second_dataset", term, authorizations);
  }

  private static List<Term> thirdDataset(String term, Authorizations authorizations) {
    return all("third_dataset", term, authorizations);
  }

  private static List<Term> all(String dataset, String term, Authorizations authorizations) {

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

  private static FieldCard fieldCardFirst(int field, Authorizations authorizations) {
    return fieldCard("first_dataset", "field_" + field, authorizations);
  }

  private static FieldCard fieldCardSecond(int field, Authorizations authorizations) {
    return fieldCard("second_dataset", "field_" + field, authorizations);
  }

  private static FieldCard fieldCardThird(int field, Authorizations authorizations) {
    return fieldCard("third_dataset", "field_" + field, authorizations);
  }

  private static FieldCard fieldCard(String dataset, String field, Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) {
      Iterator<FieldCard> iterator = termStore.fieldCard(scanner, dataset, Sets.newHashSet(field));
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private static FieldCount fieldCountFirst(int field, Authorizations authorizations) {
    return fieldCount("first_dataset", "field_" + field, authorizations);
  }

  private static FieldCount fieldCountSecond(int field, Authorizations authorizations) {
    return fieldCount("second_dataset", "field_" + field, authorizations);
  }

  private static FieldCount fieldCountThird(int field, Authorizations authorizations) {
    return fieldCount("third_dataset", "field_" + field, authorizations);
  }

  private static FieldCount fieldCount(String dataset, String field,
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

  private static FieldLabels fieldLabelsFirst(int field, Authorizations authorizations) {
    return fieldLabels("first_dataset", "field_" + field, authorizations);
  }

  private static FieldLabels fieldLabelsSecond(int field, Authorizations authorizations) {
    return fieldLabels("second_dataset", "field_" + field, authorizations);
  }

  private static FieldLabels fieldLabelsThird(int field, Authorizations authorizations) {
    return fieldLabels("third_dataset", "field_" + field, authorizations);
  }

  private static FieldLabels fieldLabels(String dataset, String field,
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

  @Before
  public void initMethods() throws Exception {
    if (termStore.isReady()) {
      boolean isOk = termStore.destroy();
    }
    if (termStore.create()) {
      fill();
    }
  }

  @Test
  public void testAddLocalityGroup() {

    Assert.assertTrue(
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").isEmpty());

    Assert.assertTrue(termStore.addLocalityGroup("third_dataset"));
    Assert.assertEquals(6,
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").size());

    Assert.assertTrue(termStore.addLocalityGroup("third_dataset")); // ensure reentrant
    Assert.assertEquals(6,
        Tables.getLocalityGroups(termStore.configurations().tableOperations(), "terms").size());
  }

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
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(i, auths));
      Assert.assertNotNull(fieldCardSecond(i, auths));
      Assert.assertNotNull(fieldCardThird(i, auths));

      Assert.assertNotNull(fieldCountFirst(i, auths));
      Assert.assertNotNull(fieldCountSecond(i, auths));
      Assert.assertNotNull(fieldCountThird(i, auths));

      Assert.assertNotNull(fieldLabelsFirst(i, auths));
      Assert.assertNotNull(fieldLabelsSecond(i, auths));
      Assert.assertNotNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(i, auths));
      Assert.assertEquals(1, countSecond(i, auths));
      Assert.assertEquals(1, countThird(i, auths));
    }

    Assert.assertTrue(termStore.truncate());

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCardFirst(i, auths));
      Assert.assertNull(fieldCardSecond(i, auths));
      Assert.assertNull(fieldCardThird(i, auths));

      Assert.assertNull(fieldCountFirst(i, auths));
      Assert.assertNull(fieldCountSecond(i, auths));
      Assert.assertNull(fieldCountThird(i, auths));

      Assert.assertNull(fieldLabelsFirst(i, auths));
      Assert.assertNull(fieldLabelsSecond(i, auths));
      Assert.assertNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(0, countFirst(i, auths));
      Assert.assertEquals(0, countSecond(i, auths));
      Assert.assertEquals(0, countThird(i, auths));
    }
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(i, auths));
      Assert.assertNotNull(fieldCardSecond(i, auths));
      Assert.assertNotNull(fieldCardThird(i, auths));

      Assert.assertNotNull(fieldCountFirst(i, auths));
      Assert.assertNotNull(fieldCountSecond(i, auths));
      Assert.assertNotNull(fieldCountThird(i, auths));

      Assert.assertNotNull(fieldLabelsFirst(i, auths));
      Assert.assertNotNull(fieldLabelsSecond(i, auths));
      Assert.assertNotNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(i, auths));
      Assert.assertEquals(1, countSecond(i, auths));
      Assert.assertEquals(1, countThird(i, auths));
    }

    try (BatchDeleter deleter = termStore.deleter(auths)) {
      Assert.assertTrue(termStore.removeDataset(deleter, "first_dataset"));
      Assert.assertTrue(termStore.removeDataset(deleter, "second_dataset"));
    }

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCardFirst(i, auths));
      Assert.assertNull(fieldCardSecond(i, auths));
      Assert.assertNotNull(fieldCardThird(i, auths));

      Assert.assertNull(fieldCountFirst(i, auths));
      Assert.assertNull(fieldCountSecond(i, auths));
      Assert.assertNotNull(fieldCountThird(i, auths));

      Assert.assertNull(fieldLabelsFirst(i, auths));
      Assert.assertNull(fieldLabelsSecond(i, auths));
      Assert.assertNotNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(0, countFirst(i, auths));
      Assert.assertEquals(0, countSecond(i, auths));
      Assert.assertEquals(1, countThird(i, auths));
    }
  }

  @Test
  public void testRemoveTerms() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCardFirst(i, auths));
      Assert.assertNotNull(fieldCardSecond(i, auths));
      Assert.assertNotNull(fieldCardThird(i, auths));

      Assert.assertNotNull(fieldCountFirst(i, auths));
      Assert.assertNotNull(fieldCountSecond(i, auths));
      Assert.assertNotNull(fieldCountThird(i, auths));

      Assert.assertNotNull(fieldLabelsFirst(i, auths));
      Assert.assertNotNull(fieldLabelsSecond(i, auths));
      Assert.assertNotNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(1, countFirst(i, auths));
      Assert.assertEquals(1, countSecond(i, auths));
      Assert.assertEquals(1, countThird(i, auths));
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
        Assert.assertNotNull(fieldCardSecond(i, auths));
        Assert.assertNotNull(fieldCountSecond(i, auths));
        Assert.assertNotNull(fieldLabelsSecond(i, auths));

        // TODO : Assert.assertNull(fieldCardFirst(i, auths));
        // TODO : Assert.assertNull(fieldCountFirst(i, auths));
        // TODO : Assert.assertNull(fieldLabelsFirst(i, auths));

        // Test terms
        Assert.assertEquals(1, countSecond(i, auths));
        Assert.assertEquals(0, countFirst(i, auths));
      } else {

        // Test metadata
        Assert.assertNotNull(fieldCardFirst(i, auths));
        Assert.assertNotNull(fieldCountFirst(i, auths));
        Assert.assertNotNull(fieldLabelsFirst(i, auths));

        // TODO : Assert.assertNull(fieldCardSecond(i, auths));
        // TODO : Assert.assertNull(fieldCountSecond(i, auths));
        // TODO : Assert.assertNull(fieldLabelsSecond(i, auths));

        // Test terms
        Assert.assertEquals(1, countFirst(i, auths));
        Assert.assertEquals(0, countSecond(i, auths));
      }

      // Test metadata
      Assert.assertNotNull(fieldCardThird(i, auths));
      Assert.assertNotNull(fieldCountThird(i, auths));
      Assert.assertNotNull(fieldLabelsThird(i, auths));

      // Test terms
      Assert.assertEquals(1, countThird(i, auths));
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1);

    // OK
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, countFirst(i, authsDS1));
      Assert.assertEquals(0, countSecond(i, authsDS1));
      Assert.assertEquals(1, countThird(i, authsDS1));
    }

    // KO (throws an exception)
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(0, countFirst(i, authsDS2));
      Assert.assertEquals(1, countSecond(i, authsDS2));
      Assert.assertEquals(1, countThird(i, authsDS2));
    }
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, authsDS1DS2);

    for (int i = 0; i < 10; i++) {

      Assert.assertEquals(1, countFirst(i, authsDS1DS2));
      Assert.assertEquals(1, countSecond(i, authsDS1DS2));
      Assert.assertEquals(1, countThird(i, authsDS1DS2));

      Assert.assertEquals(1, countFirst(i, authsDS1));
      Assert.assertEquals(0, countSecond(i, authsDS1));
      Assert.assertEquals(1, countThird(i, authsDS1));

      Assert.assertEquals(0, countFirst(i, authsDS2));
      Assert.assertEquals(1, countSecond(i, authsDS2));
      Assert.assertEquals(1, countThird(i, authsDS2));
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

      Assert.assertNotNull(fieldCardFirst(i, authsFirst));
      Assert.assertNull(fieldCardSecond(i, authsFirst));
      Assert.assertNull(fieldCardThird(i, authsFirst));

      Assert.assertNotNull(fieldCountFirst(i, authsFirst));
      Assert.assertNull(fieldCountSecond(i, authsFirst));
      Assert.assertNull(fieldCountThird(i, authsFirst));

      Assert.assertNotNull(fieldLabelsFirst(i, authsFirst));
      Assert.assertNull(fieldLabelsSecond(i, authsFirst));
      Assert.assertNull(fieldLabelsThird(i, authsFirst));

      Assert.assertNull(fieldCardFirst(i, authsSecond));
      Assert.assertNotNull(fieldCardSecond(i, authsSecond));
      Assert.assertNull(fieldCardThird(i, authsSecond));

      Assert.assertNull(fieldCountFirst(i, authsSecond));
      Assert.assertNotNull(fieldCountSecond(i, authsSecond));
      Assert.assertNull(fieldCountThird(i, authsSecond));

      Assert.assertNull(fieldLabelsFirst(i, authsSecond));
      Assert.assertNotNull(fieldLabelsSecond(i, authsSecond));
      Assert.assertNull(fieldLabelsThird(i, authsSecond));

      Assert.assertNull(fieldCardFirst(i, authsThird));
      Assert.assertNull(fieldCardSecond(i, authsThird));
      Assert.assertNotNull(fieldCardThird(i, authsThird));

      Assert.assertNull(fieldCountFirst(i, authsThird));
      Assert.assertNull(fieldCountSecond(i, authsThird));
      Assert.assertNotNull(fieldCountThird(i, authsThird));

      Assert.assertNull(fieldLabelsFirst(i, authsThird));
      Assert.assertNull(fieldLabelsSecond(i, authsThird));
      Assert.assertNotNull(fieldLabelsThird(i, authsThird));

      Assert.assertNotNull(fieldCardFirst(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCardSecond(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCardThird(i, authsFirstSecondThird));

      Assert.assertNotNull(fieldCountFirst(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountSecond(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountThird(i, authsFirstSecondThird));

      Assert.assertNotNull(fieldLabelsFirst(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsSecond(i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsThird(i, authsFirstSecondThird));
    }
  }

  @Test
  public void testFieldCard() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldCard fc1 = fieldCardFirst(i, auths);
      FieldCard fc2 = fieldCardSecond(i, auths);
      FieldCard fc3 = fieldCardThird(i, auths);

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
  }

  @Test
  public void testFieldCount() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldCount fc1 = fieldCountFirst(i, auths);
      FieldCount fc2 = fieldCountSecond(i, auths);
      FieldCount fc3 = fieldCountThird(i, auths);

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
  }

  @Test
  public void testFieldLabels() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_CARD",
        "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD", "SECOND_DATASET_VIZ",
        "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    for (int i = 0; i < 10; i++) {

      FieldLabels fl1 = fieldLabelsFirst(i, auths);
      FieldLabels fl2 = fieldLabelsSecond(i, auths);
      FieldLabels fl3 = fieldLabelsThird(i, auths);

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
  }

  @Test
  public void testTermCountExactMatch() throws Exception {

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
  }

  @Test
  public void testTermCountPrefixMatch() throws Exception {

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
  }

  @Test
  public void testTermCountSuffixMatch() throws Exception {

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
  }

  @Test
  public void testTermCountInfixMatch() throws Exception {

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
  }

  @Test
  public void testTermCardExactMatch() throws Exception {

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
  }

  @Test
  public void testTermCardPrefixMatch() throws Exception {

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
  }

  @Test
  public void testTermCardSuffixMatch() throws Exception {

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
  }

  @Test
  public void testTermCardInfixMatch() throws Exception {

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
  }

  @Test
  public void testTermScanExactMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term_1", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      Term term = list.get(0).getSecond().get(0);

      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
    }
  }

  @Test
  public void testTermScanPrefixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term_*", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {

        Assert.assertEquals("term_" + i, list.get(i).getFirst());
        Assert.assertEquals(1, list.get(i).getSecond().size());

        Term term = list.get(i).getSecond().get(0);

        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("field_" + i, term.field());
        Assert.assertEquals("term_" + i, term.term());
        Assert.assertEquals(2, term.count());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
        Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, ("term_" + i).length()),
            new ComparablePair<>(10, 10 + ("term_" + i).length())), term.spans());
      }
    }
  }

  @Test
  public void testTermScanSuffixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "*_1", null, null).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      Term term = list.get(0).getSecond().get(0);

      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
    }
  }

  @Test
  public void testTermScanInfixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term?1", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals("term_1", list.get(0).getFirst());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      Term term = list.get(0).getSecond().get(0);

      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
    }
  }
}
