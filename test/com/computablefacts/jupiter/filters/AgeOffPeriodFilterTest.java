package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import com.google.errorprone.annotations.Var;

public class AgeOffPeriodFilterTest {

  @Test
  public void testNull() {

    AgeOffPeriodFilter iterator = new AgeOffPeriodFilter();
    IteratorSetting iteratorSetting = new IteratorSetting(1, AgeOffPeriodFilter.class);
    AgeOffPeriodFilter.setTtl(iteratorSetting, 0);
    AgeOffPeriodFilter.setTtlUnits(iteratorSetting, null);

    Assert.assertFalse(iterator.validateOptions(iteratorSetting.getOptions()));
  }

  @Test
  public void testDays() throws Exception {

    AgeOffPeriodFilter iterator = iterator(5, "DAYS");

    @Var
    int nbMatchDataset1 = 0;
    @Var
    int nbMatchDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();

      if ("DATASET_1".equals(cf)) {
        nbMatchDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        nbMatchDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(1, nbMatchDataset1);
    Assert.assertEquals(2, nbMatchDataset2);
  }

  @Test
  public void testHours() throws Exception {

    AgeOffPeriodFilter iterator = iterator(5, "HOURS");

    @Var
    int nbMatchDataset1 = 0;
    @Var
    int nbMatchDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();

      if ("DATASET_1".equals(cf)) {
        nbMatchDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        nbMatchDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(0, nbMatchDataset1);
    Assert.assertEquals(0, nbMatchDataset2);
  }

  @Test
  public void testMinutes() throws Exception {

    AgeOffPeriodFilter iterator = iterator(24 * 60, "MINUTES");

    @Var
    int nbMatchDataset1 = 0;
    @Var
    int nbMatchDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();

      if ("DATASET_1".equals(cf)) {
        nbMatchDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        nbMatchDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(1, nbMatchDataset1);
    Assert.assertEquals(2, nbMatchDataset2);
  }

  @Test
  public void testSeconds() throws Exception {

    AgeOffPeriodFilter iterator = iterator(0, "SECONDS");

    @Var
    int nbMatchDataset1 = 0;
    @Var
    int nbMatchDataset2 = 0;

    while (iterator.hasTop()) {

      String cf = iterator.getTopKey().getColumnFamily().toString();

      if ("DATASET_1".equals(cf)) {
        nbMatchDataset1++;
      }
      if ("DATASET_2".equals(cf)) {
        nbMatchDataset2++;
      }

      iterator.next();
    }

    Assert.assertEquals(0, nbMatchDataset1);
    Assert.assertEquals(0, nbMatchDataset2);
  }

  private AgeOffPeriodFilter iterator(long ttl, String ttlUnits) throws IOException {

    AgeOffPeriodFilter iterator = new AgeOffPeriodFilter();
    IteratorSetting setting = new IteratorSetting(1, AgeOffPeriodFilter.class);
    AgeOffPeriodFilter.setTtl(setting, ttl);
    AgeOffPeriodFilter.setTtlUnits(setting, ttlUnits);

    Assert.assertTrue(iterator.validateOptions(setting.getOptions()));

    iterator.init(new SortedMapIterator(map()), setting.getOptions(), null);
    iterator.seek(new Range(), Collections.EMPTY_LIST, false);

    return iterator;
  }

  private SortedMap<Key, Value> map() {

    LocalDateTime dateMinus10Days = LocalDateTime.now().minusDays(10);
    LocalDateTime dateMinus10Hours = LocalDateTime.now().minusHours(10);

    SortedMap<Key, Value> map = new TreeMap<>();

    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_1",
        dateMinus10Days.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_2",
        dateMinus10Days.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("1"));
    map.put(new Key("TERM_1", "DATASET_1", "DOCID_1\0FIELD_3",
        dateMinus10Hours.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("1"));

    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_1",
        dateMinus10Hours.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_2",
        dateMinus10Hours.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("2"));
    map.put(new Key("TERM_2", "DATASET_2", "DOCID_2\0FIELD_3",
        dateMinus10Days.toInstant(ZoneOffset.UTC).toEpochMilli()), new Value("2"));

    return map;
  }
}
