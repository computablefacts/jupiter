package com.computablefacts.jupiter.storage.termstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      termStore.counts(scanner, dataset, null, "john").forEachRemaining(termsCounts::add);

      Assert.assertEquals(1, termsCounts.size());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "john", null, null).forEachRemaining(terms::add);

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
      termStore.counts(scanner, dataset, null, "john").forEachRemaining(termsCounts::add);

      Assert.assertTrue(termsCounts.isEmpty());

      List<Term> terms = new ArrayList<>();
      termStore.bucketsIds(scanner, dataset, "john", null, null).forEachRemaining(terms::add);

      Assert.assertTrue(terms.isEmpty());
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
