package com.computablefacts.jupiter.storage.datastore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.Users;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.jupiter.storage.termstore.FieldCard;
import com.computablefacts.jupiter.storage.termstore.FieldCount;
import com.computablefacts.jupiter.storage.termstore.FieldLabels;
import com.computablefacts.jupiter.storage.termstore.IngestStats;
import com.computablefacts.jupiter.storage.termstore.Term;
import com.computablefacts.jupiter.storage.termstore.TermCard;
import com.computablefacts.jupiter.storage.termstore.TermCount;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

/**
 * This class is not thread-safe because {@link MiniAccumuloClusterUtils#setUserAuths} is used. Do
 * not execute methods in parallel.
 */
@net.jcip.annotations.NotThreadSafe
public class DataStoreTest {

  private static MiniAccumuloCluster accumulo;
  private static Configurations configurations;
  private static DataStore dataStore;

  @BeforeClass
  public static void initClass() throws Exception {
    accumulo = MiniAccumuloClusterUtils.newCluster();
    configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    dataStore = new DataStore(configurations, "data_store");
  }

  @AfterClass
  public static void uinitClass() throws Exception {
    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  private static void fill() {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    try (Writers writers = dataStore.writers()) {
      try (IngestStats stats = dataStore.newIngestStats()) {

        for (int i = 0; i < 10; i++) {
          Assert
              .assertTrue(dataStore.persist(writers, stats, "first_dataset", "row_" + i, json1(i)));
        }

        for (int i = 0; i < 10; i++) {
          Assert.assertTrue(
              dataStore.persist(writers, stats, "second_dataset", "row_" + i, json1(i)));
        }

        for (int i = 0; i < 10; i++) {
          Assert
              .assertTrue(dataStore.persist(writers, stats, "third_dataset", "row_" + i, json1(i)));
        }
      }
    }
  }

  private static String json1(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }]}";
  }

  private static String json2(int i) {
    return "{\n" + "  \"uuid\": " + i + ",\n" + "  \"Actors\": [\n" + "    {\n"
        + "      \"uuid\": \"item" + i + "0\",\n" + "      \"name\": \"Tom Cruise\",\n"
        + "      \"age\": 56,\n" + "      \"Born At\": \"Syracuse, NY\",\n"
        + "      \"Birthdate\": \"July 3, 1962\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\",\n"
        + "      \"wife\": null,\n" + "      \"weight\": 67.5,\n" + "      \"hasChildren\": true,\n"
        + "      \"hasGreyHair\": false,\n" + "      \"children\": [\n" + "        \"Suri\",\n"
        + "        \"Isabella Jane\",\n" + "        \"Connor\"\n" + "      ]\n" + "    },\n"
        + "    {\n" + "      \"uuid\": \"item" + i + "1\",\n"
        + "      \"name\": \"Robert Downey Jr.\",\n" + "      \"age\": 53,\n"
        + "      \"Born At\": \"New York City, NY\",\n"
        + "      \"Birthdate\": \"April 4, 1965\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/Robert-Downey-Jr.jpg\",\n"
        + "      \"wife\": \"Susan Downey\",\n" + "      \"weight\": 77.1,\n"
        + "      \"hasChildren\": true,\n" + "      \"hasGreyHair\": false,\n"
        + "      \"children\": [\n" + "        \"Indio Falconer\",\n" + "        \"Avri Roel\",\n"
        + "        \"Exton Elias\"\n" + "      ]\n" + "    }\n" + "  ]\n" + "}";
  }

  private static int countFirst(Authorizations authorizations) {
    return count("first_dataset", authorizations);
  }

  private static int countSecond(Authorizations authorizations) {
    return count("second_dataset", authorizations);
  }

  private static int countThird(Authorizations authorizations) {
    return count("third_dataset", authorizations);
  }

  private static int count(String dataset, Authorizations authorizations) {
    return all(dataset, authorizations).size();
  }

  private static List<Blob<Value>> firstDataset(Authorizations authorizations) {
    return all("first_dataset", authorizations);
  }

  private static List<Blob<Value>> secondDataset(Authorizations authorizations) {
    return all("second_dataset", authorizations);
  }

  private static List<Blob<Value>> thirdDataset(Authorizations authorizations) {
    return all("third_dataset", authorizations);
  }

  private static List<Blob<Value>> all(String dataset, Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) { // keep order

      List<Blob<Value>> list = new ArrayList<>();
      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, dataset);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private static int countFirst(String term, Set<String> fields, Authorizations authorizations) {
    return count("first_dataset", term, fields, authorizations);
  }

  private static int countSecond(String term, Set<String> fields, Authorizations authorizations) {
    return count("second_dataset", term, fields, authorizations);
  }

  private static int countThird(String term, Set<String> fields, Authorizations authorizations) {
    return count("third_dataset", term, fields, authorizations);
  }

  private static int count(String dataset, String term, Set<String> fields,
      Authorizations authorizations) {
    return all(dataset, term, fields, authorizations).size();
  }

  private static List<Pair<String, List<Term>>> firstDataset(String term, Set<String> fields,
      Authorizations authorizations) {
    return all("first_dataset", term, fields, authorizations);
  }

  private static List<Pair<String, List<Term>>> secondDataset(String term, Set<String> fields,
      Authorizations authorizations) {
    return all("second_dataset", term, fields, authorizations);
  }

  private static List<Pair<String, List<Term>>> thirdDataset(String term, Set<String> fields,
      Authorizations authorizations) {
    return all("third_dataset", term, fields, authorizations);
  }

  private static List<Pair<String, List<Term>>> all(String dataset, String term, Set<String> fields,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(term, "term should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) { // keep order

      List<Pair<String, List<Term>>> list = new ArrayList<>();
      Iterator<Pair<String, List<Term>>> iterator =
          dataStore.termScan(scanners, dataset, term, fields, null);

      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }

  private static FieldCard fieldCardFirst(String field, Authorizations authorizations) {
    return fieldCard("first_dataset", field, authorizations);
  }

  private static FieldCard fieldCardSecond(String field, Authorizations authorizations) {
    return fieldCard("second_dataset", field, authorizations);
  }

  private static FieldCard fieldCardThird(String field, Authorizations authorizations) {
    return fieldCard("third_dataset", field, authorizations);
  }

  private static FieldCard fieldCard(String dataset, String field, Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldCard> iterator = dataStore.fieldCard(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private static FieldCount fieldCountFirst(String field, Authorizations authorizations) {
    return fieldCount("first_dataset", field, authorizations);
  }

  private static FieldCount fieldCountSecond(String field, Authorizations authorizations) {
    return fieldCount("second_dataset", field, authorizations);
  }

  private static FieldCount fieldCountThird(String field, Authorizations authorizations) {
    return fieldCount("third_dataset", field, authorizations);
  }

  private static FieldCount fieldCount(String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldCount> iterator = dataStore.fieldCount(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private static FieldLabels fieldLabelsFirst(String field, Authorizations authorizations) {
    return fieldLabels("first_dataset", field, authorizations);
  }

  private static FieldLabels fieldLabelsSecond(String field, Authorizations authorizations) {
    return fieldLabels("second_dataset", field, authorizations);
  }

  private static FieldLabels fieldLabelsThird(String field, Authorizations authorizations) {
    return fieldLabels("third_dataset", field, authorizations);
  }

  private static FieldLabels fieldLabels(String dataset, String field,
      Authorizations authorizations) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    try (Scanners scanners = dataStore.scanners(authorizations)) {
      Iterator<FieldLabels> iterator = dataStore.fieldLabels(scanners, dataset, field);
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  private static String normalize(String str) {
    String token = Codecs.nopTokenizer.apply(str).span(0).text();
    return Codecs.nopLexicoder.apply(token).text();
  }

  @Before
  public void initMethods() {
    if (dataStore.isReady()) {
      boolean isOk = dataStore.destroy();
    }
    if (dataStore.create()) {
      fill();
    }
  }

  @Test
  public void testAddLocalityGroup() {
    // TODO
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testReadBlobStoreWithoutTablesPermissions() {

    String[] auths = new String[] {"FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9"};

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(
        Users.setAuthorizations(configurations.connector(), "jdoe", Sets.newHashSet(auths)));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Scanners scanners = ds.scanners(new Authorizations(auths))) {

      ScannerBase scanner = scanners.blob();
      scanner.fetchColumnFamily(new Text("first_dataset"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here
    }
  }

  @Test(expected = RuntimeException.class) // Should be AccumuloSecurityException?
  public void testReadTermStoreWithoutTablesPermissions() {

    String[] auths = new String[] {"FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9"};

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(
        Users.setAuthorizations(configurations.connector(), "jdoe", Sets.newHashSet(auths)));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Scanners scanners = ds.scanners(new Authorizations(auths))) {

      ScannerBase scanner = scanners.index();
      scanner.fetchColumnFamily(new Text("first_dataset_FIDX"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add); // An exception is thrown here
    }
  }

  @Test
  public void testReadBlobStoreWithTablesPermissions() {

    String[] auths = new String[] {"FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9"};

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(
        Users.setAuthorizations(configurations.connector(), "jdoe", Sets.newHashSet(auths)));
    Assert.assertTrue(dataStore.grantReadPermissions("jdoe"));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Scanners scanners = ds.scanners(new Authorizations(auths))) {

      ScannerBase scanner = scanners.blob();
      scanner.fetchColumnFamily(new Text("first_dataset"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add);

      Assert.assertEquals(10, list.size());
    }

    Assert.assertTrue(dataStore.revokeReadPermissions("jdoe"));
    Assert.assertTrue(Users.delete(configurations.connector(), "jdoe"));
  }

  @Test
  public void testReadTermStoreWithTablesPermissions() {

    String[] auths = new String[] {"FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9"};

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(
        Users.setAuthorizations(configurations.connector(), "jdoe", Sets.newHashSet(auths)));
    Assert.assertTrue(dataStore.grantReadPermissions("jdoe"));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Scanners scanners = ds.scanners(new Authorizations(auths))) {

      ScannerBase scanner = scanners.index();
      scanner.fetchColumnFamily(new Text("first_dataset_FIDX"));

      List<Map.Entry<Key, Value>> list = new ArrayList<>();
      scanner.iterator().forEachRemaining(list::add);

      Assert.assertEquals(120, list.size());
    }

    Assert.assertTrue(dataStore.revokeReadPermissions("jdoe"));
    Assert.assertTrue(Users.delete(configurations.connector(), "jdoe"));
  }

  @Test
  public void testGrantWriteOnBlobStoreThenRevoke() throws Exception {

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(dataStore.grantWritePermissionOnBlobStore("jdoe"));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Writers writers = ds.writers()) {

      Mutation m = new Mutation("test_row");
      m.put(new Text("first_dataset"), Constants.TEXT_EMPTY, Constants.VALUE_EMPTY);
      writers.blob().addMutation(m);

      Assert.assertTrue(writers.flush());
    }

    Assert.assertTrue(dataStore.revokeWritePermissionOnBlobStore("jdoe"));

    try (Writers writers = ds.writers()) {

      Mutation m = new Mutation("test_row");
      m.put(new Text("second_dataset"), Constants.TEXT_EMPTY, Constants.VALUE_EMPTY);
      writers.blob().addMutation(m);

      Assert.assertFalse(writers.flush());
    }

    Assert.assertTrue(Users.delete(configurations.connector(), "jdoe"));
  }

  @Test
  public void testGrantWriteOnTermStoreThenRevoke() throws Exception {

    Assert.assertTrue(Users.create(configurations.connector(), "jdoe", "azerty"));
    Assert.assertTrue(dataStore.grantWritePermissionOnTermStore("jdoe"));

    Configurations confs = new Configurations(configurations.instanceName(),
        configurations.zooKeepers(), "jdoe", "azerty");
    DataStore ds = new DataStore(confs, dataStore.name());

    try (Writers writers = ds.writers()) {

      Mutation m = new Mutation("test");
      m.put(new Text("first_dataset_FIDX"), new Text("000|0000-00-00T00:00:00.000Z\0field"),
          Constants.VALUE_EMPTY);
      writers.index().addMutation(m);

      Assert.assertTrue(writers.flush());
    }

    Assert.assertTrue(dataStore.revokeWritePermissionOnTermStore("jdoe"));

    try (Writers writers = ds.writers()) {

      Mutation m = new Mutation("test");
      m.put(new Text("second_dataset_FIDX"), new Text("000|0000-00-00T00:00:00.000Z\0field"),
          Constants.VALUE_EMPTY);
      writers.index().addMutation(m);

      Assert.assertFalse(writers.flush());
    }

    Assert.assertTrue(Users.delete(configurations.connector(), "jdoe"));
  }

  @Test
  public void testCreateAndIsReady() throws Exception {

    MiniAccumuloCluster accumulo = MiniAccumuloClusterUtils.newCluster();
    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo);
    TermStore termStore = new TermStore(configurations, "data_store");

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
    TermStore termStore = new TermStore(configurations, "data_store");

    Assert.assertTrue(termStore.create());
    Assert.assertTrue(termStore.isReady());

    Assert.assertTrue(termStore.destroy());
    Assert.assertFalse(termStore.isReady());

    Assert.assertTrue(termStore.destroy()); // ensure destroy is reentrant
    Assert.assertFalse(termStore.isReady());

    MiniAccumuloClusterUtils.destroyCluster(accumulo);
  }

  @Test
  public void testRemoveDataset() throws Exception {

    Authorizations auths = Constants.AUTH_ADM;
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    Assert.assertTrue(dataStore.remove("first_dataset"));
    Assert.assertTrue(dataStore.remove("second_dataset"));

    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));
  }

  @Test
  public void testRemoveRows() throws Exception {

    Authorizations auths = Constants.AUTH_ADM;
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) { // remove even rows from dataset 1
        Assert.assertTrue(dataStore.remove("first_dataset", Sets.newHashSet("row_" + i)));
      } else { // remove odd rows from dataset 2
        Assert.assertTrue(dataStore.remove("second_dataset", Sets.newHashSet("row_" + i)));
      }
    }

    Assert.assertEquals(5, countFirst(auths));
    Assert.assertEquals(5, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    // Ensure odd rows remain in dataset 1
    List<Blob<Value>> iterator1 = firstDataset(auths);

    for (int i = 0; i < 5; i++) {
      Blob<Value> blob = iterator1.get(i);
      Assert.assertEquals("row_" + (2 * i + 1), blob.key());
    }

    // Ensure even rows remain in dataset 2
    List<Blob<Value>> iterator2 = firstDataset(auths);

    for (int i = 0; i < 5; i++) {
      Blob<Value> blob = iterator2.get(i);
      Assert.assertEquals("row_" + (2 * i + 1), blob.key());
    }
  }

  @Test
  public void testTruncate() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9",
        "SECOND_DATASET_ROW_0", "SECOND_DATASET_ROW_1", "SECOND_DATASET_ROW_2",
        "SECOND_DATASET_ROW_3", "SECOND_DATASET_ROW_4", "SECOND_DATASET_ROW_5",
        "SECOND_DATASET_ROW_6", "SECOND_DATASET_ROW_7", "SECOND_DATASET_ROW_8",
        "SECOND_DATASET_ROW_9", "THIRD_DATASET_ROW_0", "THIRD_DATASET_ROW_1", "THIRD_DATASET_ROW_2",
        "THIRD_DATASET_ROW_3", "THIRD_DATASET_ROW_4", "THIRD_DATASET_ROW_5", "THIRD_DATASET_ROW_6",
        "THIRD_DATASET_ROW_7", "THIRD_DATASET_ROW_8", "THIRD_DATASET_ROW_9", "FIRST_DATASET_CNT",
        "FIRST_DATASET_CARD", "FIRST_DATASET_VIZ", "SECOND_DATASET_CNT", "SECOND_DATASET_CARD",
        "SECOND_DATASET_VIZ", "THIRD_DATASET_CNT", "THIRD_DATASET_CARD", "THIRD_DATASET_VIZ");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    // BlobStore
    Assert.assertEquals(10, countFirst(auths));
    Assert.assertEquals(10, countSecond(auths));
    Assert.assertEquals(10, countThird(auths));

    // Test TermStore's metadata
    Assert.assertNotNull(fieldCardFirst("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldCardSecond("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldCardThird("Actors[0]¤children[0]", auths));

    Assert.assertNotNull(fieldCountFirst("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldCountSecond("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldCountThird("Actors[0]¤children[0]", auths));

    Assert.assertNotNull(fieldLabelsFirst("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldLabelsSecond("Actors[0]¤children[0]", auths));
    Assert.assertNotNull(fieldLabelsThird("Actors[0]¤children[0]", auths));

    // Test TermStore's terms
    Assert.assertEquals(1, countFirst("suri", Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(1, countSecond("suri", Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(1, countThird("suri", Sets.newHashSet("Actors[0]¤children[0]"), auths));

    Assert.assertTrue(dataStore.truncate());

    // BlobStore
    Assert.assertEquals(0, countFirst(auths));
    Assert.assertEquals(0, countSecond(auths));
    Assert.assertEquals(0, countThird(auths));

    // Test TermStore's metadata
    Assert.assertNull(fieldCardFirst("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCardSecond("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCardThird("Actors[0]¤children[0]", auths));

    Assert.assertNull(fieldCountFirst("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCountSecond("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldCountThird("Actors[0]¤children[0]", auths));

    Assert.assertNull(fieldLabelsFirst("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldLabelsSecond("Actors[0]¤children[0]", auths));
    Assert.assertNull(fieldLabelsThird("Actors[0]¤children[0]", auths));

    // Test TermStore's terms
    Assert.assertEquals(0,
        countFirst(normalize("Suri"), Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(0,
        countSecond(normalize("Suri"), Sets.newHashSet("Actors[0]¤children[0]"), auths));
    Assert.assertEquals(0,
        countThird(normalize("Suri"), Sets.newHashSet("Actors[0]¤children[0]"), auths));
  }

  @Test
  public void testBlobScan() throws Exception {

    Authorizations auths1 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    Authorizations auths2 = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4");

    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths1);

    // Use the Scanner auths to filter data
    Set<Blob<Value>> list1 = new HashSet<>();

    try (Scanners scanners = dataStore.scanners(auths2)) { // keep order

      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, "first_dataset");

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list1.add(blob);
        Assert.assertEquals("row_" + (list1.size() - 1), blob.key());
      }
    }

    // Use the doc ids to filter data
    Set<Blob<Value>> list2 = new HashSet<>();

    try (Scanners scanners = new Scanners(configurations, dataStore.name(), auths1, 2)) { // out-of-order

      Iterator<Blob<Value>> iterator = dataStore.blobScan(scanners, "first_dataset",
          Sets.newHashSet("row_0", "row_1", "row_2", "row_3", "row_4"));

      while (iterator.hasNext()) {
        Blob<Value> blob = iterator.next();
        list2.add(blob);
      }
    }

    Assert.assertEquals(list1, list2);
  }

  @Test
  public void testSearchTerm() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        List<String> list = new ArrayList<>();
        dataStore.searchByTerm(scanners, writers, "first_dataset", normalize("Connor"), null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();
        dataStore.searchByTerm(scanners, writers, "first_dataset", normalize("56"), null, null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();
        dataStore.searchByTerm(scanners, writers, "first_dataset", normalize("67.5"), null, null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }
      }
    }
  }

  @Test
  public void testSearchTerms() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ROW_0", "FIRST_DATASET_ROW_1",
        "FIRST_DATASET_ROW_2", "FIRST_DATASET_ROW_3", "FIRST_DATASET_ROW_4", "FIRST_DATASET_ROW_5",
        "FIRST_DATASET_ROW_6", "FIRST_DATASET_ROW_7", "FIRST_DATASET_ROW_8", "FIRST_DATASET_ROW_9");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        List<String> list = new ArrayList<>();
        dataStore
            .searchByTerms(scanners, writers, "first_dataset",
                Sets.newHashSet(normalize("Connor"), normalize("56"), normalize("67.5")), null)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }
      }
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {

    Authorizations auths = new Authorizations("FOURTH_DATASET_ROW_0", "FOURTH_DATASET_ROW_1",
        "FOURTH_DATASET_ROW_2", "FOURTH_DATASET_ROW_3", "FOURTH_DATASET_ROW_4",
        "FOURTH_DATASET_ROW_5", "FOURTH_DATASET_ROW_6", "FOURTH_DATASET_ROW_7",
        "FOURTH_DATASET_ROW_8", "FOURTH_DATASET_ROW_9");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order
      try (Writers writers = dataStore.writers()) {

        // Create a new dataset
        try (IngestStats stats = dataStore.newIngestStats()) {
          for (int i = 0; i < 10; i++) {
            Assert.assertTrue(dataStore.persist(writers, stats, "fourth_dataset", "row_" + i,
                json2(i), null, Codecs.defaultTokenizer));
          }
        }

        Assert.assertTrue(writers.flush());

        // Test three simple queries
        List<String> list = new ArrayList<>();
        QueryBuilder.build("uuid:\"5\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(0, list.size()); // triggers "Discard small terms"...

        list.clear();

        QueryBuilder.build("Actors[*]¤uuid:\"item1*\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_1", list.get(0));

        list.clear();

        QueryBuilder.build("Actors[*]¤uuid:\"item5?\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_5", list.get(0));

        list.clear();

        // Test an array query
        QueryBuilder.build("Actors[*]¤children[*]:\"Suri\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals("row_" + i, list.get(i));
        }

        list.clear();

        // Test an AND query
        QueryBuilder.build(
            "Actors[*]¤children[*]:\"Suri\" AND Actors[0]¤uuid:\"item?0\" AND Actors[1]¤uuid:\"item5?\"")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("row_5", list.get(0));

        list.clear();

        // Test an OR query
        QueryBuilder.build(
            "Actors[*]¤children[*]:\"Roel\" AND (Actors[0]¤uuid:\"item4?\" OR Actors[0]¤uuid:\"item5?\")")
            .execute(dataStore, scanners, writers, "fourth_dataset", null, Codecs.defaultTokenizer)
            .forEachRemaining(list::add);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("row_4", list.get(0));
        Assert.assertEquals("row_5", list.get(1));

        list.clear();

        // Cleanup
        MiniAccumuloClusterUtils.setUserAuths(accumulo, Constants.AUTH_ADM);
        Assert.assertTrue(dataStore.remove("fourth_dataset"));
      }
    }
  }

  @Test
  public void testWriteReadCacheSync() {
    try (Scanners scanners = dataStore.scanners(Authorizations.EMPTY)) { // keep order
      try (Writers writers = dataStore.writers()) {

        Set<String> set = new HashSet<>();

        for (int i = 0; i < 10; i++) {
          set.add(Integer.toString(i, 10));
        }

        String cacheId = dataStore.writeCache(writers, set.iterator());

        Set<String> setNew = new HashSet<>();
        dataStore.readCache(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        dataStore.readCache(scanners, cacheId, "5").forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("5", "6", "7", "8", "9"), setNew);
      }
    }
  }

  @Test
  public void testWriteReadCacheAsync() throws Exception {
    try (Scanners scanners = dataStore.scanners(Authorizations.EMPTY)) { // keep order
      try (Writers writers = dataStore.writers()) {

        Set<String> set = new HashSet<>();

        for (int i = 0; i < 10; i++) {
          set.add(Integer.toString(i, 10));
        }

        String cacheId = dataStore.writeCache(writers, set.iterator(), 5);

        Thread.sleep(1000);

        Set<String> setNew = new HashSet<>();
        dataStore.readCache(scanners, cacheId).forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            setNew);

        setNew.clear();
        dataStore.readCache(scanners, cacheId, "5").forEachRemaining(setNew::add);

        Assert.assertEquals(Sets.newHashSet("5", "6", "7", "8", "9"), setNew);
      }
    }
  }

  @Test
  public void testTermCount() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<TermCount>>> list = new ArrayList<>();
      dataStore.termCount(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      @Var
      TermCount tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.count());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Single dataset, hits backward index
      list.clear();
      dataStore.termCount(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.count());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Cross datasets
      list.clear();
      dataStore.termCount(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(3, list.get(0).getSecond().size());

      for (int i = 0; i < 3; i++) {

        tc = list.get(0).getSecond().get(i);

        Assert.assertEquals("Actors[0]¤children[2]", tc.field());
        Assert.assertEquals("connor", tc.term());
        Assert.assertEquals(10, tc.count());

        if (i == 0) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
              tc.labels());
        } else if (i == 1) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_"),
              tc.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_"),
              tc.labels());
        }
      }
    }
  }

  @Test
  public void testTermCard() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<TermCard>>> list = new ArrayList<>();
      dataStore.termCard(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      @Var
      TermCard tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Single dataset, hits backward index
      list.clear();
      dataStore.termCard(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(1, list.get(0).getSecond().size());

      tc = list.get(0).getSecond().get(0);

      Assert.assertEquals("Actors[0]¤children[2]", tc.field());
      Assert.assertEquals("connor", tc.term());
      Assert.assertEquals(10, tc.cardinality());
      Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
          tc.labels());

      // Cross datasets
      list.clear();
      dataStore.termCard(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(3, list.get(0).getSecond().size());

      for (int i = 0; i < 3; i++) {

        tc = list.get(0).getSecond().get(i);

        Assert.assertEquals("Actors[0]¤children[2]", tc.field());
        Assert.assertEquals("connor", tc.term());
        Assert.assertEquals(10, tc.cardinality());

        if (i == 0) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_"),
              tc.labels());
        } else if (i == 1) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_"),
              tc.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_"),
              tc.labels());
        }
      }
    }
  }

  @Test
  public void testTermScan() throws Exception {

    Authorizations auths = new Authorizations("FIRST_DATASET_ACTORS_0_", "SECOND_DATASET_ACTORS_0_",
        "THIRD_DATASET_ACTORS_0_");
    MiniAccumuloClusterUtils.setUserAuths(accumulo, auths);

    try (Scanners scanners = dataStore.scanners(auths)) { // keep order

      // Single dataset, hits forward index
      List<Pair<String, List<Term>>> list = new ArrayList<>();
      dataStore.termScan(scanners, "first_dataset", normalize("Conno?"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(10, list.get(0).getSecond().size());

      for (int i = 0; i < 10; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());
        Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
            "FIRST_DATASET_ROW_" + i), term.labels());
      }

      // Single dataset, hits backward index
      list.clear();
      dataStore.termScan(scanners, "first_dataset", normalize("?onnor"))
          .forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(10, list.get(0).getSecond().size());

      for (int i = 0; i < 10; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + i, term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());
        Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
            "FIRST_DATASET_ROW_" + i), term.labels());
      }

      // Cross datasets
      list.clear();
      dataStore.termScan(scanners, null, normalize("Connor")).forEachRemaining(list::add);

      Assert.assertEquals(1, list.size());
      Assert.assertEquals(30, list.get(0).getSecond().size());

      for (int i = 0; i < 30; i++) {

        Term term = list.get(0).getSecond().get(i);

        Assert.assertEquals("row_" + (i % 10), term.docId());
        Assert.assertEquals("Actors[0]¤children[2]", term.field());
        Assert.assertEquals("connor", term.term());
        Assert.assertEquals(1, term.count());

        if (i < 10) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "FIRST_DATASET_ACTORS_0_",
              "FIRST_DATASET_ROW_" + (i % 10)), term.labels());
        } else if (i < 20) {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "SECOND_DATASET_ACTORS_0_",
              "SECOND_DATASET_ROW_" + (i % 10)), term.labels());
        } else {
          Assert.assertEquals(Sets.newHashSet(Constants.STRING_ADM, "THIRD_DATASET_ACTORS_0_",
              "THIRD_DATASET_ROW_" + (i % 10)), term.labels());
        }
      }
    }
  }
}
