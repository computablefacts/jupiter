package com.computablefacts.jupiter.iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.nona.helpers.Codecs;
import com.google.errorprone.annotations.Var;

public class BlobStoreMaskingIteratorIllegalQuotedCharacterTest {

  @Test
  public void testWithoutIllegalQuotedCharacterInFilteredFields() throws Exception {

    BlobStoreMaskingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DATASET_1_NAME", "DATASET_1_AGE"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String row = iterator.getTopKey().getRow().toString();
      String value = iterator.getTopValue().toString();

      if (row.startsWith("DATASET_1")) {
        Assert.assertEquals(Codecs.asObject(
            "{\"name\":\"John\",\"city\":\"MASKED_4e6b610487d59bc2c6ee3642e988f8e2\",\"age\":31}"),
            Codecs.asObject(value));
        countDataset1++;
      }
      if (row.startsWith("DATASET_2")) {
        Assert.assertEquals(Codecs.asObject(
            "{\"name\":\"MASKED_58a8d7d6cfe7a6c919ae22551a37be8f\",\"city\":\"MASKED_4e6b610487d59bc2c6ee3642e988f8e2\",\"age\":\"MASKED_eba47ab112ed4342e5ea8848e9262dea\"}"),
            Codecs.asObject(value));
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testFiltersWithIllegalQuotedCharacterInFilteredFields() throws Exception {

    BlobStoreMaskingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DATASET_1_NAME", "DATASET_1_CITY"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String row = iterator.getTopKey().getRow().toString();
      String value = iterator.getTopValue().toString();

      if (row.startsWith("DATASET_1")) {
        Assert.assertEquals(Codecs.asObject(
            "{\"name\":\"John\",\"city\":\"New\\u0007York\",\"age\":\"MASKED_eba47ab112ed4342e5ea8848e9262dea\"}"),
            Codecs.asObject(value));
        countDataset1++;
      }
      if (row.startsWith("DATASET_2")) {
        Assert.assertEquals(Codecs.asObject(
            "{\"name\":\"MASKED_58a8d7d6cfe7a6c919ae22551a37be8f\",\"city\":\"MASKED_4e6b610487d59bc2c6ee3642e988f8e2\",\"age\":\"MASKED_eba47ab112ed4342e5ea8848e9262dea\"}"),
            Codecs.asObject(value));
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  private BlobStoreMaskingIterator iterator(Authorizations auths) throws IOException {

    BlobStoreMaskingIterator iterator = new BlobStoreMaskingIterator();
    IteratorSetting setting = new IteratorSetting(1, BlobStoreMaskingIterator.class);
    BlobStoreMaskingIterator.setAuthorizations(setting, auths);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(
        new Key("DATASET_1\0KEY_1", "", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));
    map.put(
        new Key("DATASET_1\0KEY_2", "", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));
    map.put(
        new Key("DATASET_1\0KEY_3", "", "3\0", new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0),
        new Value(json()));

    map.put(
        new Key("DATASET_2\0KEY_1", "", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));
    map.put(
        new Key("DATASET_2\0KEY_2", "", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));
    map.put(
        new Key("DATASET_2\0KEY_3", "", "3\0", new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0),
        new Value(json()));

    return map;
  }

  private String json() {
    return "{\"name\":\"John\", \"age\":31, \"city\":\"New\\u0007York\"}";
  }
}
