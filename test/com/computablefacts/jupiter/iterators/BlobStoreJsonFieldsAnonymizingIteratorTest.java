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
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.google.errorprone.annotations.Var;

public class BlobStoreJsonFieldsAnonymizingIteratorTest {

  @Test
  public void testNullAuthorizations() {

    BlobStoreJsonFieldsAnonymizingIterator iterator = new BlobStoreJsonFieldsAnonymizingIterator();
    IteratorSetting iteratorSetting =
        new IteratorSetting(1, BlobStoreJsonFieldsAnonymizingIterator.class);
    BlobStoreJsonFieldsAnonymizingIterator.setAuthorizations(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testNoMatchingAuthorizations() throws Exception {

    BlobStoreJsonFieldsAnonymizingIterator iterator = iterator(new Authorizations());

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testOnlyAdmAuthorization() throws Exception {

    BlobStoreJsonFieldsAnonymizingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testOnlyRawDataAuthorization() throws Exception {

    BlobStoreJsonFieldsAnonymizingIterator iterator =
        iterator(new Authorizations("DATASET_1_RAW_DATA"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals(json(), value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testTwoMatchingAuthorizations1() throws Exception {

    BlobStoreJsonFieldsAnonymizingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DATASET_1_AGE", "DATASET_1_CITY"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"age\":31,\"city\":\"New York\"}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"is_anonymized\":\"true\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  @Test
  public void testTwoMatchingAuthorizations2() throws Exception {

    BlobStoreJsonFieldsAnonymizingIterator iterator =
        iterator(new Authorizations(Constants.STRING_ADM, "DATASET_1_AGE", "DATASET_2_CITY"));

    @Var
    int countDataset1 = 0;
    @Var
    int countDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();
      String value = iterator.getTopValue().toString();

      if ("DATASET_1".equals(cf)) {
        Assert.assertEquals("{\"age\":31}", value);
        countDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        Assert.assertEquals("{\"city\":\"New York\"}", value);
        countDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(3, countDataset1);
    Assert.assertEquals(3, countDataset2);
  }

  private BlobStoreJsonFieldsAnonymizingIterator iterator(Authorizations auths) throws IOException {

    BlobStoreJsonFieldsAnonymizingIterator iterator = new BlobStoreJsonFieldsAnonymizingIterator();
    IteratorSetting setting = new IteratorSetting(1, BlobStoreJsonFieldsAnonymizingIterator.class);
    BlobStoreJsonFieldsAnonymizingIterator.setAuthorizations(setting, auths);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("KEY_1", "DATASET_1", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0), new Value(json()));
    map.put(new Key("KEY_2", "DATASET_1", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0), new Value(json()));
    map.put(new Key("KEY_3", "DATASET_1", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_1_RAW_DATA"), 0), new Value(json()));

    map.put(new Key("KEY_1", "DATASET_2", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0), new Value(json()));
    map.put(new Key("KEY_2", "DATASET_2", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0), new Value(json()));
    map.put(new Key("KEY_3", "DATASET_2", Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL,
        new ColumnVisibility("ADM|DATASET_2_RAW_DATA"), 0), new Value(json()));

    return map;
  }

  private String json() {
    return "{\"name\":\"John\", \"age\":31, \"city\":\"New York\"}";
  }
}
