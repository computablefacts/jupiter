package com.computablefacts.jupiter;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.computablefacts.nona.helpers.Codecs;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Data {

  public static Map<String, Object> json(int id) {
    return Codecs.asObject(
        "{" + "  \"uuid\": " + id + "," + "  \"Actors\": [" + "    {" + "      \"uuid\": \"item_"
            + id + "_1\"," + "      \"name\": \"Tom Cruise\"," + "      \"age\": 56,"
            + "      \"Born At\": \"Syracuse, NY\"," + "      \"Birthdate\": \"July 3, 1962\","
            + "      \"photo\": \"https://jsonformatter.org/img/tom-cruise.jpg\","
            + "      \"wife\": null," + "      \"weight\": 67.5," + "      \"hasChildren\": true,"
            + "      \"hasGreyHair\": false," + "      \"children\": [" + "        \"Suri\","
            + "        \"Isabella Jane\"," + "        \"Connor\"" + "      ]" + "    }," + "    {"
            + "      \"uuid\": \"item_" + id + "_2\"," + "      \"name\": \"Robert Downey Jr.\","
            + "      \"age\": 73," + "      \"Born At\": \"New York City, NY\","
            + "      \"Birthdate\": \"April 4, 1965\","
            + "      \"photo\": \"https://jsonformatter.org/img/Robert-Downey-Jr.jpg\","
            + "      \"wife\": \"Susan Downey\"," + "      \"weight\": 77.1,"
            + "      \"hasChildren\": true," + "      \"hasGreyHair\": false,"
            + "      \"children\": [" + "        \"Indio Falconer\"," + "        \"Avri Roel\","
            + "        \"Exton Elias\"" + "      ]" + "    }" + "  ]" + "}");
  }

  public static Map<String, Object> json2(int id) throws Exception {
    Map<String, Object> json = new HashMap<>();
    json.put("id", Integer.toString(id, 10));
    json.put("first_name", "john");
    json.put("last_name", "doe");
    json.put("age", 17);
    json.put("birthdate", new SimpleDateFormat("yyyy-MM-dd").parse("2004-04-01"));
    return json;
  }

  public static Map<String, Object> json3(int id) throws Exception {
    Map<String, Object> json = new HashMap<>();
    json.put("id", Integer.toString(id, 10));
    json.put("first_name", "jane");
    json.put("last_name", "doe");
    json.put("age", 18);
    json.put("birthdate", new SimpleDateFormat("yyyy-MM-dd").parse("2003-04-01"));
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
