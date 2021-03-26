package com.computablefacts.jupiter.storage.datastore;

import java.util.Iterator;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.computablefacts.jupiter.MiniAccumuloClusterUtils;
import com.computablefacts.jupiter.queries.AbstractNode;
import com.computablefacts.jupiter.queries.QueryBuilder;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.Codecs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.Var;

public class DataStoreDefaultTokenizerTest extends MiniAccumuloClusterTest {

  @Test
  public void testAndQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("(tom AND crui*) AND (rob* AND down*)");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset",
            null, Codecs.defaultTokenizer);

        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.defaultTokenizer);

        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testAndNotQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("(tom AND crui*) AND NOT (rob* OR down*)");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset",
            null, Codecs.defaultTokenizer);

        Assert.assertEquals(10, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.defaultTokenizer);

        Assert.assertEquals(0, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testOrQuery() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("(tom AND crui*) OR (rob* AND down*)");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset",
            null, Codecs.defaultTokenizer);

        Assert.assertEquals(10, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.defaultTokenizer);

        Assert.assertEquals(10, Iterators.size(iterator));
      }
    }
  }

  @Test
  public void testAndQueryWithSmallTermToBeFixedAsap() throws Exception {

    DataStore dataStore = newDataStore(Constants.AUTH_ADM);
    AbstractNode query = QueryBuilder.build("(tom AND crui*) AND (rob* AND jr.)");

    try (Scanners scanners = dataStore.scanners(Constants.AUTH_ADM)) {
      try (Writers writers = dataStore.writers()) {

        @Var
        Iterator<String> iterator = query.execute(dataStore, scanners, writers, "first_dataset",
            null, Codecs.defaultTokenizer);

        Assert.assertEquals(0, Iterators.size(iterator));

        iterator = query.execute(dataStore, scanners, writers, "second_dataset", null,
            Codecs.defaultTokenizer);

        // Because the small term "jr" is pruned in TerminalNode and a Constants.ITERATOR_EMPTY is
        // returned :-(
        Assert.assertEquals(0, Iterators.size(iterator));
      }
    }
  }

  private void fillDataStore(DataStore dataStore) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    try (Writers writers = dataStore.writers()) {

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "first_dataset", "row_" + i, json1(i), null,
            Codecs.defaultTokenizer));
      }

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(dataStore.persist(writers, "second_dataset", "row_" + i, json2(i), null,
            Codecs.defaultTokenizer));
      }
    }
  }

  private DataStore newDataStore(Authorizations auths) throws Exception {
    String username = nextUsername();
    return newDataStore(auths, username);
  }

  private DataStore newDataStore(Authorizations auths, String username) throws Exception {

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
        + "      \"uuid\": \"item" + i + "0\",\n" + "      \"name\": \"Tom\\n\\r\\t Cruise\",\n"
        + "      \"age\": 56,\n" + "      \"Born At\": \"Syracuse, NY\",\n"
        + "      \"Birthdate\": \"July 3, 1962\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\",\n"
        + "      \"wife\": null,\n" + "      \"weight\": 67.5,\n" + "      \"hasChildren\": true,\n"
        + "      \"hasGreyHair\": false,\n" + "      \"children\": [\n" + "        \"Suri\",\n"
        + "        \"Isabella Jane\",\n" + "        \"Connor\"\n" + "      ]\n" + "    },\n"
        + "    {\n" + "      \"uuid\": \"item" + i + "1\",\n"
        + "      \"name\": \"Robert \\t\\r\\nDowney Jr.\",\n" + "      \"age\": 53,\n"
        + "      \"Born At\": \"New York City, NY\",\n"
        + "      \"Birthdate\": \"April 4, 1965\",\n"
        + "      \"photo\": \"https://jsonformatter.org/img/Robert-Downey-Jr.jpg\",\n"
        + "      \"wife\": \"Susan Downey\",\n" + "      \"weight\": 77.1,\n"
        + "      \"hasChildren\": true,\n" + "      \"hasGreyHair\": false,\n"
        + "      \"children\": [\n" + "        \"Indio Falconer\",\n" + "        \"Avri Roel\",\n"
        + "        \"Exton Elias\"\n" + "      ]\n" + "    }\n" + "  ]\n" + "}";
  }
}
