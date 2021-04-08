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
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.BigDecimalCodec;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TermStoreTest extends MiniAccumuloClusterTest {

  @Test
  public void testAddLocalityGroup() throws Exception {

    TermStore termStore = newDataStore(Constants.AUTH_ADM);

    Assert.assertTrue(Tables
        .getLocalityGroups(termStore.configurations().tableOperations(), termStore.tableName())
        .isEmpty());

    Assert.assertTrue(termStore.addLocalityGroup("third_dataset"));
    Assert.assertEquals(4,
        Tables
            .getLocalityGroups(termStore.configurations().tableOperations(), termStore.tableName())
            .size());

    Assert.assertTrue(termStore.addLocalityGroup("third_dataset")); // ensure reentrant
    Assert.assertEquals(4,
        Tables
            .getLocalityGroups(termStore.configurations().tableOperations(), termStore.tableName())
            .size());
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    TermStore termStore = new TermStore(configurations, tableName);

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    Assert.assertTrue(termStore.create()); // ensure create is reentrant
    Assert.assertTrue(termStore.isReady());
  }

  @Test
  public void testCreateAndDestroy() throws Exception {

    String tableName = nextTableName();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo());
    TermStore termStore = new TermStore(configurations, tableName);

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());

    Assert.assertTrue(termStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(termStore.isReady());
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths =
        new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT", "FIRST_DATASET_VIZ",
            "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      FieldCount fco1 = fieldCountInFirstDataset(termStore, i, auths);
      FieldCount fco2 = fieldCountInSecondDataset(termStore, i, auths);
      FieldCount fco3 = fieldCountInThirdDataset(termStore, i, auths);

      Assert.assertEquals("field_" + i, fco1.field());
      Assert.assertEquals(Sets.newHashSet("FIRST_DATASET_CNT", "ADM"), fco1.labels());
      Assert.assertEquals(1, fco1.count());
      Assert.assertTrue(fco1.isString());

      Assert.assertEquals("field_" + i, fco2.field());
      Assert.assertEquals(Sets.newHashSet("SECOND_DATASET_CNT", "ADM"), fco2.labels());
      Assert.assertEquals(1, fco2.count());
      Assert.assertTrue(fco2.isString());

      Assert.assertEquals("field_" + i, fco3.field());
      Assert.assertEquals(Sets.newHashSet("THIRD_DATASET_CNT", "ADM"), fco3.labels());
      Assert.assertEquals(2, fco3.count());
      Assert.assertTrue(fco3.isString());

      FieldLabels fl1 = fieldLabelsInFirstDataset(termStore, i, auths);
      FieldLabels fl2 = fieldLabelsInSecondDataset(termStore, i, auths);
      FieldLabels fl3 = fieldLabelsInThirdDataset(termStore, i, auths);

      Assert.assertEquals("field_" + i, fl1.field());
      Assert.assertEquals(Sets.newHashSet("FIRST_DATASET_VIZ", "ADM"), fl1.labels());
      Assert.assertEquals(Sets.newHashSet("DS_1"), fl1.termLabels());
      Assert.assertTrue(fl1.isString());

      Assert.assertEquals("field_" + i, fl2.field());
      Assert.assertEquals(Sets.newHashSet("SECOND_DATASET_VIZ", "ADM"), fl2.labels());
      Assert.assertEquals(Sets.newHashSet("DS_2"), fl2.termLabels());
      Assert.assertTrue(fl2.isString());

      Assert.assertEquals("field_" + i, fl3.field());
      Assert.assertEquals(Sets.newHashSet("THIRD_DATASET_VIZ", "ADM"), fl3.labels());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), fl3.termLabels());
      Assert.assertTrue(fl3.isString());

      // Test terms
      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, auths));
    }

    Assert.assertTrue(termStore.truncate());

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCountInFirstDataset(termStore, i, auths));
      Assert.assertNull(fieldCountInSecondDataset(termStore, i, auths));
      Assert.assertNull(fieldCountInThirdDataset(termStore, i, auths));

      Assert.assertNull(fieldLabelsInFirstDataset(termStore, i, auths));
      Assert.assertNull(fieldLabelsInSecondDataset(termStore, i, auths));
      Assert.assertNull(fieldLabelsInThirdDataset(termStore, i, auths));

      // Test terms
      Assert.assertEquals(0, countEntitiesInFirstDataset(termStore, i, auths));
      Assert.assertEquals(0, countEntitiesInSecondDataset(termStore, i, auths));
      Assert.assertEquals(0, countEntitiesInThirdDataset(termStore, i, auths));
    }
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths =
        new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT", "FIRST_DATASET_VIZ",
            "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCountInFirstDataset(termStore, i, auths));
      Assert.assertNotNull(fieldCountInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, auths));

      Assert.assertNotNull(fieldLabelsInFirstDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, auths));
    }

    try (BatchDeleter deleter = termStore.deleter(auths)) {
      Assert.assertTrue(termStore.removeDataset(deleter, "first_dataset"));
      Assert.assertTrue(termStore.removeDataset(deleter, "second_dataset"));
    }

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNull(fieldCountInFirstDataset(termStore, i, auths));
      Assert.assertNull(fieldCountInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, auths));

      Assert.assertNull(fieldLabelsInFirstDataset(termStore, i, auths));
      Assert.assertNull(fieldLabelsInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, auths));

      // Test terms
      Assert.assertEquals(0, countEntitiesInFirstDataset(termStore, i, auths));
      Assert.assertEquals(0, countEntitiesInSecondDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, auths));
    }
  }

  @Test
  public void testRemoveTerms() throws Exception {

    Authorizations auths =
        new Authorizations("DS_1", "DS_2", "FIRST_DATASET_CNT", "FIRST_DATASET_VIZ",
            "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      // Test metadata
      Assert.assertNotNull(fieldCountInFirstDataset(termStore, i, auths));
      Assert.assertNotNull(fieldCountInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, auths));

      Assert.assertNotNull(fieldLabelsInFirstDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInSecondDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, auths));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, auths));
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
        Assert.assertNotNull(fieldCountInSecondDataset(termStore, i, auths));
        Assert.assertNotNull(fieldLabelsInSecondDataset(termStore, i, auths));

        // TODO : Assert.assertNull(fieldCountFirst(termStore, i, auths));
        // TODO : Assert.assertNull(fieldLabelsFirst(termStore, i, auths));

        // Test terms
        Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, auths));
        Assert.assertEquals(0, countEntitiesInFirstDataset(termStore, i, auths));
      } else {

        // Test metadata
        Assert.assertNotNull(fieldCountInFirstDataset(termStore, i, auths));
        Assert.assertNotNull(fieldLabelsInFirstDataset(termStore, i, auths));

        // TODO : Assert.assertNull(fieldCountSecond(termStore, i, auths));
        // TODO : Assert.assertNull(fieldLabelsSecond(termStore, i, auths));

        // Test terms
        Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, auths));
        Assert.assertEquals(0, countEntitiesInSecondDataset(termStore, i, auths));
      }

      // Test metadata
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, auths));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, auths));

      // Test terms
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, auths));
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testVisibilityLabelsUserHasMissingAuths() throws Exception {

    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    TermStore termStore = newDataStore(authsDS1);

    // OK
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, authsDS1));
      Assert.assertEquals(0, countEntitiesInSecondDataset(termStore, i, authsDS1));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, authsDS1));
    }

    // KO (throws an exception)
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(0, countEntitiesInFirstDataset(termStore, i, authsDS2));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, authsDS2));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, authsDS2));
    }
  }

  @Test
  public void testVisibilityLabelsUserHasAllAuths() throws Exception {

    Authorizations authsDS1DS2 = new Authorizations("DS_1", "DS_2");
    Authorizations authsDS1 = new Authorizations("DS_1");
    Authorizations authsDS2 = new Authorizations("DS_2");

    String username = nextUsername();
    TermStore termStore = newDataStore(authsDS1DS2, username);

    for (int i = 0; i < 10; i++) {

      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, authsDS1DS2));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, authsDS1DS2));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, authsDS1DS2));

      Assert.assertEquals(1, countEntitiesInFirstDataset(termStore, i, authsDS1));
      Assert.assertEquals(0, countEntitiesInSecondDataset(termStore, i, authsDS1));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, authsDS1));

      Assert.assertEquals(0, countEntitiesInFirstDataset(termStore, i, authsDS2));
      Assert.assertEquals(1, countEntitiesInSecondDataset(termStore, i, authsDS2));
      Assert.assertEquals(1, countEntitiesInThirdDataset(termStore, i, authsDS2));
    }

    Authorizations authsFirstSecondThird =
        new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT",
            "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");

    Authorizations authsFirst = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_VIZ");

    Authorizations authsSecond = new Authorizations("SECOND_DATASET_CNT", "SECOND_DATASET_VIZ");

    Authorizations authsThird = new Authorizations("THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");

    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, authsFirstSecondThird);

    for (int i = 0; i < 10; i++) {

      Assert.assertNotNull(fieldCountInFirstDataset(termStore, i, authsFirst));
      Assert.assertNull(fieldCountInSecondDataset(termStore, i, authsFirst));
      Assert.assertNull(fieldCountInThirdDataset(termStore, i, authsFirst));

      Assert.assertNotNull(fieldLabelsInFirstDataset(termStore, i, authsFirst));
      Assert.assertNull(fieldLabelsInSecondDataset(termStore, i, authsFirst));
      Assert.assertNull(fieldLabelsInThirdDataset(termStore, i, authsFirst));

      Assert.assertNull(fieldCountInFirstDataset(termStore, i, authsSecond));
      Assert.assertNotNull(fieldCountInSecondDataset(termStore, i, authsSecond));
      Assert.assertNull(fieldCountInThirdDataset(termStore, i, authsSecond));

      Assert.assertNull(fieldLabelsInFirstDataset(termStore, i, authsSecond));
      Assert.assertNotNull(fieldLabelsInSecondDataset(termStore, i, authsSecond));
      Assert.assertNull(fieldLabelsInThirdDataset(termStore, i, authsSecond));

      Assert.assertNull(fieldCountInFirstDataset(termStore, i, authsThird));
      Assert.assertNull(fieldCountInSecondDataset(termStore, i, authsThird));
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, authsThird));

      Assert.assertNull(fieldLabelsInFirstDataset(termStore, i, authsThird));
      Assert.assertNull(fieldLabelsInSecondDataset(termStore, i, authsThird));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, authsThird));

      Assert.assertNotNull(fieldCountInFirstDataset(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountInSecondDataset(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldCountInThirdDataset(termStore, i, authsFirstSecondThird));

      Assert.assertNotNull(fieldLabelsInFirstDataset(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsInSecondDataset(termStore, i, authsFirstSecondThird));
      Assert.assertNotNull(fieldLabelsInThirdDataset(termStore, i, authsFirstSecondThird));
    }
  }

  @Test
  public void testFieldCount() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_VIZ",
        "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      FieldCount fc1 = fieldCountInFirstDataset(termStore, i, auths);
      FieldCount fc2 = fieldCountInSecondDataset(termStore, i, auths);
      FieldCount fc3 = fieldCountInThirdDataset(termStore, i, auths);

      Assert.assertEquals("field_" + i, fc1.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_CNT"), fc1.labels());
      Assert.assertEquals(1, fc1.count());
      Assert.assertTrue(fc1.isString());

      Assert.assertEquals("field_" + i, fc2.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_CNT"),
          fc2.labels());
      Assert.assertEquals(1, fc2.count());
      Assert.assertTrue(fc2.isString());

      Assert.assertEquals("field_" + i, fc3.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_CNT"), fc3.labels());
      Assert.assertEquals(2, fc3.count());
      Assert.assertTrue(fc3.isString());
    }
  }

  @Test
  public void testFieldLabels() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_CNT", "FIRST_DATASET_VIZ",
        "SECOND_DATASET_CNT", "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_VIZ");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      FieldLabels fl1 = fieldLabelsInFirstDataset(termStore, i, auths);
      FieldLabels fl2 = fieldLabelsInSecondDataset(termStore, i, auths);
      FieldLabels fl3 = fieldLabelsInThirdDataset(termStore, i, auths);

      Assert.assertEquals("field_" + i, fl1.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_VIZ"),
          fl1.labels());
      Assert.assertEquals(Sets.newHashSet("DS_1"), fl1.termLabels());
      Assert.assertTrue(fl1.isString());

      Assert.assertEquals("field_" + i, fl2.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_VIZ"),
          fl2.labels());
      Assert.assertEquals(Sets.newHashSet("DS_2"), fl2.termLabels());
      Assert.assertTrue(fl2.isString());

      Assert.assertEquals("field_" + i, fl3.field());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_VIZ"),
          fl3.labels());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), fl3.termLabels());
      Assert.assertTrue(fl3.isString());
    }
  }

  @Test
  public void testFieldLastUpdate() throws Exception {

    Authorizations auths =
        new Authorizations("FIRST_DATASET_LU", "SECOND_DATASET_LU", "THIRD_DATASET_LU");
    TermStore termStore = newDataStore(auths);

    for (int i = 0; i < 10; i++) {

      FieldLastUpdate flu1 = fieldLastUpdateInFirstDataset(termStore, i, auths);
      FieldLastUpdate flu2 = fieldLastUpdateInSecondDataset(termStore, i, auths);
      FieldLastUpdate flu3 = fieldLastUpdateInThirdDataset(termStore, i, auths);

      Assert.assertEquals("field_" + i, flu1.field());
      Assert.assertTrue(WildcardMatcher.match(flu1.lastUpdate(), "????-??-??T??:??:??*Z"));
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_LU"), flu1.labels());
      Assert.assertTrue(flu1.isString());

      Assert.assertEquals("field_" + i, flu2.field());
      Assert.assertTrue(WildcardMatcher.match(flu2.lastUpdate(), "????-??-??T??:??:??*Z"));
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_LU"),
          flu2.labels());
      Assert.assertTrue(flu2.isString());

      Assert.assertEquals("field_" + i, flu3.field());
      Assert.assertTrue(WildcardMatcher.match(flu3.lastUpdate(), "????-??-??T??:??:??*Z"));
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_LU"), flu3.labels());
      Assert.assertTrue(flu3.isString());
    }
  }

  @Test
  public void testTermCountExactMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      TermCount tc = list.get(0);

      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), tc.labels());
      Assert.assertTrue(tc.isString());
    }
  }

  @Test
  public void testTermCountPrefixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term_*").forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {

        TermCount tc = list.get(i);

        Assert.assertEquals("term_" + i, tc.term());
        Assert.assertEquals("field_" + i, tc.field());
        Assert.assertEquals("term_" + i, tc.term());
        Assert.assertEquals(2, tc.count());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), tc.labels());
        Assert.assertTrue(tc.isString());
      }
    }
  }

  @Test
  public void testTermCountSuffixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "*_1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      TermCount tc = list.get(0);

      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), tc.labels());
      Assert.assertTrue(tc.isString());
    }
  }

  @Test
  public void testTermCountInfixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      termStore.termCount(scanner, "third_dataset", "term?1").forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      TermCount tc = list.get(0);

      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals("field_1", tc.field());
      Assert.assertEquals("term_1", tc.term());
      Assert.assertEquals(2, tc.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), tc.labels());
      Assert.assertTrue(tc.isString());
    }
  }

  @Test
  public void testTermScanExactMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term_1", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      Term term = list.get(0);

      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
      Assert.assertTrue(term.isString());
    }
  }

  @Test
  public void testTermScanPrefixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term_*", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());

      for (int i = 0; i < 10; i++) {

        Term term = list.get(i);

        Assert.assertEquals("term_" + i, list.get(i).term());
        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("field_" + i, term.field());
        Assert.assertEquals("term_" + i, term.term());
        Assert.assertEquals(2, term.count());
        Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
        Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, ("term_" + i).length()),
            new ComparablePair<>(10, 10 + ("term_" + i).length())), term.spans());
        Assert.assertTrue(term.isString());
      }
    }
  }

  @Test
  public void testTermScanSuffixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "*_1", null, null).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      Term term = list.get(0);

      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
      Assert.assertTrue(term.isString());
    }
  }

  @Test
  public void testTermScanInfixMatch() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      termStore.termScan(scanner, "third_dataset", "term?1", null, null)
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());

      Term term = list.get(0);

      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals("row_1", term.docId());
      Assert.assertEquals("field_1", term.field());
      Assert.assertEquals("term_1", term.term());
      Assert.assertEquals(2, term.count());
      Assert.assertEquals(Sets.newHashSet("DS_1", "DS_2"), term.labels());
      Assert.assertEquals(Lists.newArrayList(new ComparablePair<>(0, "term_1".length()),
          new ComparablePair<>(10, 10 + "term_1".length())), term.spans());
      Assert.assertTrue(term.isString());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNumericalRangeScanOpenedBeginOpenedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      // throws an exception
      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator =
          termStore.numericalRangeScan(scanner, "fourth_dataset", null, null, null, null);
    }
  }

  @Test
  public void testNumericalRangeScanClosedBeginClosedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator =
          termStore.numericalRangeScan(scanner, "fourth_dataset", "3", "8", null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(5, list.size());

      Assert.assertEquals("3", list.get(0).term());
      Assert.assertEquals("4", list.get(1).term());
      Assert.assertEquals("5", list.get(2).term());
      Assert.assertEquals("6", list.get(3).term());
      Assert.assertEquals("7", list.get(4).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
    }
  }

  @Test
  public void testNumericalRangeScanClosedBeginOpenedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator =
          termStore.numericalRangeScan(scanner, "fourth_dataset", "3", null, null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(7, list.size());

      Assert.assertEquals("3", list.get(0).term());
      Assert.assertEquals("4", list.get(1).term());
      Assert.assertEquals("5", list.get(2).term());
      Assert.assertEquals("6", list.get(3).term());
      Assert.assertEquals("7", list.get(4).term());
      Assert.assertEquals("8", list.get(5).term());
      Assert.assertEquals("9", list.get(6).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
      Assert.assertTrue(list.get(5).isNumber());
      Assert.assertTrue(list.get(6).isNumber());
    }
  }

  @Test
  public void testNumericalRangeScanOpenedBeginClosedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator =
          termStore.numericalRangeScan(scanner, "fourth_dataset", null, "8", null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(8, list.size());

      Assert.assertEquals("0", list.get(0).term());
      Assert.assertEquals("1", list.get(1).term());
      Assert.assertEquals("2", list.get(2).term());
      Assert.assertEquals("3", list.get(3).term());
      Assert.assertEquals("4", list.get(4).term());
      Assert.assertEquals("5", list.get(5).term());
      Assert.assertEquals("6", list.get(6).term());
      Assert.assertEquals("7", list.get(7).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
      Assert.assertTrue(list.get(5).isNumber());
      Assert.assertTrue(list.get(6).isNumber());
      Assert.assertTrue(list.get(7).isNumber());
    }
  }

  @Test
  public void testNumericalRangeCountClosedBeginClosedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      Iterator<TermCount> iterator =
          termStore.numericalRangeCount((ScannerBase) scanner, "fourth_dataset", "3", "8", null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(5, list.size());

      Assert.assertEquals("3", list.get(0).term());
      Assert.assertEquals("4", list.get(1).term());
      Assert.assertEquals("5", list.get(2).term());
      Assert.assertEquals("6", list.get(3).term());
      Assert.assertEquals("7", list.get(4).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
    }
  }

  @Test
  public void testNumericalRangeCountClosedBeginOpenedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      Iterator<TermCount> iterator =
          termStore.numericalRangeCount((ScannerBase) scanner, "fourth_dataset", "3", null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(7, list.size());

      Assert.assertEquals("3", list.get(0).term());
      Assert.assertEquals("4", list.get(1).term());
      Assert.assertEquals("5", list.get(2).term());
      Assert.assertEquals("6", list.get(3).term());
      Assert.assertEquals("7", list.get(4).term());
      Assert.assertEquals("8", list.get(5).term());
      Assert.assertEquals("9", list.get(6).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
      Assert.assertTrue(list.get(5).isNumber());
      Assert.assertTrue(list.get(6).isNumber());
    }
  }

  @Test
  public void testNumericalRangeCountOpenedBeginClosedEnd() throws Exception {

    Authorizations auths = new Authorizations("DS_1", "DS_2");
    TermStore termStore = newDataStore(auths);

    try (Scanner scanner = termStore.scanner(auths)) {

      List<TermCount> list = new ArrayList<>();
      Iterator<TermCount> iterator =
          termStore.numericalRangeCount((ScannerBase) scanner, "fourth_dataset", null, "8", null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      Assert.assertEquals(8, list.size());

      Assert.assertEquals("0", list.get(0).term());
      Assert.assertEquals("1", list.get(1).term());
      Assert.assertEquals("2", list.get(2).term());
      Assert.assertEquals("3", list.get(3).term());
      Assert.assertEquals("4", list.get(4).term());
      Assert.assertEquals("5", list.get(5).term());
      Assert.assertEquals("6", list.get(6).term());
      Assert.assertEquals("7", list.get(7).term());

      Assert.assertTrue(list.get(0).isNumber());
      Assert.assertTrue(list.get(1).isNumber());
      Assert.assertTrue(list.get(2).isNumber());
      Assert.assertTrue(list.get(3).isNumber());
      Assert.assertTrue(list.get(4).isNumber());
      Assert.assertTrue(list.get(5).isNumber());
      Assert.assertTrue(list.get(6).isNumber());
      Assert.assertTrue(list.get(7).isNumber());
    }
  }

  private FieldLastUpdate fieldLastUpdateInFirstDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLastUpdate(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldLastUpdate fieldLastUpdateInSecondDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLastUpdate(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldLastUpdate fieldLastUpdateInThirdDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLastUpdate(termStore, "third_dataset", "field_" + field, authorizations);
  }

  private FieldLastUpdate fieldLastUpdate(TermStore termStore, String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) {
      Iterator<FieldLastUpdate> iterator =
          termStore.fieldLastUpdate(scanner, dataset, Sets.newHashSet(field));
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private FieldLabels fieldLabelsInFirstDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLabels(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldLabels fieldLabelsInSecondDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldLabels(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldLabels fieldLabelsInThirdDataset(TermStore termStore, int field,
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

  private FieldCount fieldCountInFirstDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldCount(termStore, "first_dataset", "field_" + field, authorizations);
  }

  private FieldCount fieldCountInSecondDataset(TermStore termStore, int field,
      Authorizations authorizations) {
    return fieldCount(termStore, "second_dataset", "field_" + field, authorizations);
  }

  private FieldCount fieldCountInThirdDataset(TermStore termStore, int field,
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

  private int countEntitiesInFirstDataset(TermStore termStore, int term,
      Authorizations authorizations) {
    return countEntities(termStore, "first_dataset", term, authorizations);
  }

  private int countEntitiesInSecondDataset(TermStore termStore, int term,
      Authorizations authorizations) {
    return countEntities(termStore, "second_dataset", term, authorizations);
  }

  private int countEntitiesInThirdDataset(TermStore termStore, int term,
      Authorizations authorizations) {
    return countEntities(termStore, "third_dataset", term, authorizations);
  }

  private int countEntities(TermStore termStore, String dataset, int term,
      Authorizations authorizations) {
    return entities(termStore, dataset, "term_" + term, authorizations).size();
  }

  private List<Term> entities(TermStore termStore, String dataset, String term,
      Authorizations authorizations) {

    Preconditions.checkNotNull(termStore, "termStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    try (Scanner scanner = termStore.scanner(authorizations)) { // keep order

      List<Term> list = new ArrayList<>();
      Iterator<Term> iterator = termStore.termScan(scanner, dataset, term, null, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private void fillDataStore(TermStore termStore) throws Exception {

    Preconditions.checkNotNull(termStore, "termStore should not be null");

    try (BatchWriter writer = termStore.writer()) {
      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            termStore.add(writer, "first_dataset", "row_" + i, "field_" + i, Term.TYPE_STRING,
                "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
                Sets.newHashSet(), Sets.newHashSet("DS_1")));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(
            termStore.add(writer, "second_dataset", "row_" + i, "field_" + i, Term.TYPE_STRING,
                "term_" + i, Lists.newArrayList(new Pair<>(0, ("term_" + i).length())),
                Sets.newHashSet(), Sets.newHashSet("DS_2")));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(termStore.add(writer, "third_dataset", "row_" + i, "field_" + i,
            Term.TYPE_STRING, "term_" + i,
            Lists.newArrayList(new Pair<>(0, ("term_" + i).length()),
                new Pair<>(10, 10 + ("term_" + i).length())),
            Sets.newHashSet(), Sets.newHashSet("DS_1", "DS_2")));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(termStore.add(writer, "fourth_dataset", "row_" + i, "field_" + i,
            Term.TYPE_NUMBER, BigDecimalCodec.encode(Integer.toString(i)),
            Lists.newArrayList(new Pair<>(0, ("term_" + i).length()),
                new Pair<>(10, 10 + ("term_" + i).length())),
            Sets.newHashSet(), Sets.newHashSet("DS_1", "DS_2"), true));
      }
    }
  }

  private TermStore newDataStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newDataStore(auths, username);
  }

  private TermStore newDataStore(Authorizations auths, String username) throws Exception {

    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    TermStore termStore = new TermStore(configurations, tableName);

    if (termStore.create()) {
      fillDataStore(termStore);
    }

    MiniAccumuloClusterUtils.setUserTablePermissions(accumulo(), username, tableName);

    return termStore;
  }
}
