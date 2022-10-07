package com.computablefacts.jupiter;

import com.computablefacts.asterix.codecs.JsonCodec;
import com.google.errorprone.annotations.CheckReturnValue;
import java.io.File;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@CheckReturnValue
final public class Data {

  public static Map<String, Object> json(int id) {
    return JsonCodec.asObject(
        "{" + "  \"uuid\": " + id + "," + "  \"Actors\": [" + "    {" + "      \"uuid\": \"item_" + id + "_1\","
            + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56," + "      \"Born At\": \"Syracuse, NY\","
            + "      \"Birthdate\": \"July 3, 1962\","
            + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\"," + "      \"wife\": null,"
            + "      \"weight\": 67.5," + "      \"hasChildren\": true," + "      \"hasGreyHair\": false,"
            + "      \"children\": [" + "        \"Suri\"," + "        \"Isabella Jane\"," + "        \"Connor\""
            + "      ]" + "    }," + "    {" + "      \"uuid\": \"item_" + id + "_2\","
            + "      \"name\": \"Robert Downey Jr.\"," + "      \"age\": 73,"
            + "      \"Born At\": \"New York City, NY\"," + "      \"Birthdate\": \"April 4, 1965\","
            + "      \"photo\": \"https://jsonformatter.org/img/Robert-Downey-Jr.jpg\","
            + "      \"wife\": \"Susan Downey\"," + "      \"weight\": 77.1," + "      \"hasChildren\": true,"
            + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Indio Falconer\","
            + "        \"Avri Roel\"," + "        \"Exton Elias\"" + "      ]" + "    }" + "  ]" + "}");
  }

  public static Map<String, Object> json2(int id) throws Exception {
    Map<String, Object> json = new HashMap<>();
    json.put("id", Integer.toString(id, 10));
    json.put("first_name", "john");
    json.put("last_name", "doe");
    json.put("age", 17);
    json.put("birthdate", Date.from(Instant.parse("2004-04-01T00:00:00Z")));
    return json;
  }

  public static Map<String, Object> json3(int id) throws Exception {
    Map<String, Object> json = new HashMap<>();
    json.put("id", Integer.toString(id, 10));
    json.put("first_name", "jane");
    json.put("last_name", "doe");
    json.put("age", 18);
    json.put("birthdate", Date.from(Instant.parse("2003-04-01T00:00:00Z")));
    return json;
  }

  public static Map<String, Object> json4() {
    Map<String, Object> json = new HashMap<>();
    json.put("id", "0001");
    json.put("nom", "doe");
    json.put("prénom", "john");
    json.put("age", 27);
    json.put("sexe", "F");
    json.put("rue", 95);
    json.put("adresse", "avenue Charles-de-Gaulle");
    json.put("code_postal", "01800");
    json.put("ville", "Villieu");
    json.put("nb_connexions", "0");
    return json;
  }

  public static Map<String, Object> json5() {
    Map<String, Object> json = new HashMap<>();
    json.put("path", "/opt/test/01_MyFile.csv");
    return json;
  }

  public static Map<String, Object> json6() {
    Map<String, Object> json = new HashMap<>();
    json.put("id", "1");
    json.put("nom", "doe");
    json.put("prénom", "john");
    json.put("score_1", "0.98");
    json.put("score_2", ".99");
    json.put("score_3", "97.");
    return json;
  }

  public static File file(int size) throws Exception {
    java.io.File file = java.io.File.createTempFile("tmp-", ".txt");
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.setLength(size);
    }
    return file;
  }
}
