package com.computablefacts.jupiter.queries;

import java.util.Iterator;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.Var;

public class QueryExecutionTest extends MiniAccumuloClusterTest {

  @Test
  public void testPruneSmallTerm() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("uuid:1");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertFalse(iterator.hasNext()); // value length < 3
      }
    }
  }

  @Test
  public void testInflectionalQueryWithoutWildcard() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("Actors[*]¤uuid:item");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(20,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testLiteralQueryWithoutWildcard() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("Actors[*]¤uuid:\"item\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testInflectionalQueryWithWildcard() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("Actors[*]¤uuid:item*");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(20,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testLiteralQueryWithWildcard() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("Actors[*]¤uuid:\"item*\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(20,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testAndQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND Actors[*]¤uuid:\"item?0\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testOrQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom OR Actors[*]¤uuid:\"item?0\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(20,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testAndNotQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom AND -Actors[*]¤uuid:\"item?0\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testOrNotQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("tom OR -Actors[*]¤uuid:\"item?0\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testQueryHitsReverseIndexAndReturnMoreThanOneResult() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    @Var
    AbstractNode query = QueryBuilder.build("Actors[*]¤uuid:\"item?0\" AND Actors[*]¤name:*cruise");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }

    query =
        QueryBuilder.build("NOT(NOT(Actors[*]¤uuid:\"item?0\") OR NOT(Actors[*]¤name:*cruise))");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testQueryHitsReverseIndexAndReturnNoResult() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    @Var
    AbstractNode query =
        QueryBuilder.build("Actors[*]¤uuid:\"item?0\" AND Actors[*]¤name:\"*downey jr.\"");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }

    query = QueryBuilder
        .build("NOT(NOT(Actors[*]¤uuid:\"item?0\") OR NOT(Actors[*]¤name:\"*downey jr.\"))");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator =
            query.execute(dataStore, scanners, writers, "first_dataset", null, Codecs.nopTokenizer);

        Assert.assertEquals(0,
            query.cardinality(dataStore, scanners, "first_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.nopTokenizer);

        Assert.assertEquals(10,
            query.cardinality(dataStore, scanners, "second_dataset", Codecs.nopTokenizer));
        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  private void fillDataStore(DataStore dataStore) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    try (Writers writers = dataStore.writers()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "first_dataset", "row_" + i, json1(i),
            key -> true, Codecs.nopTokenizer, Codecs.nopLexicoder));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "second_dataset", "row_" + i, json2(i),
            key -> true, Codecs.nopTokenizer, Codecs.nopLexicoder));
      }
    }
  }

  private DataStore newDataStore(Authorizations auths) throws Exception {

    String username = nextUsername();
    String tableName = nextTableName();

    MiniAccumuloClusterUtils.newUser(accumulo(), username);
    MiniAccumuloClusterUtils.setUserAuths(accumulo(), username, auths);
    MiniAccumuloClusterUtils.setUserSystemPermissions(accumulo(), username);

    Configurations configurations = MiniAccumuloClusterUtils.newConfiguration(accumulo(), username);
    DataStore dataStore = new DataStore(configurations, tableName);

    if (dataStore.create()) {
      fillDataStore(dataStore);
    }

    boolean isOk = dataStore.grantReadPermissions(username);

    return dataStore;
  }

  private String json1(int i) {
    return "{" + "  \"Actors\": [" + "    {" + "      \"uuid\": " + i + ","
        + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
        + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
        + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
        + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
        + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }]}";
  }

  private String json2(int i) {
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
}
