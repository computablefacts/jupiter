package com.computablefacts.jupiter.filters;

import com.computablefacts.asterix.Generated;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

@CheckReturnValue
public class AgeOffPeriodFilter extends Filter {

  private static final String START_DATE = "sd";
  private static final String TTL = "ttl";
  private static final String TTL_UNITS = "ttlu";

  private long startDate_;
  private long cutOffMillis_;

  public static void setStartDate(IteratorSetting setting, long startDate) {
    if (startDate > 0) {
      setting.addOption(START_DATE, Long.toString(startDate, 10));
    }
  }

  public static void setTtl(IteratorSetting setting, long ttl) {
    setting.addOption(TTL, Long.toString(ttl, 10));
  }

  public static void setTtlUnits(IteratorSetting setting, String ttlUnits) {
    if (ttlUnits != null) {
      setting.addOption(TTL_UNITS, ttlUnits);
    }
  }

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(START_DATE, "Start date");
    options.put(TTL, "TTL");
    options.put(TTL_UNITS, "Units");

    return new IteratorOptions("AgeOffPeriodFilter",
        "AgeOffPeriodFilter accepts or rejects each Key/Value pair based on its timestamp.", options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {

    if (options.size() < 2) {
      return false;
    }

    @Var boolean hasTtl = false;
    @Var boolean hasTtlUnits = false;

    for (String option : options.keySet()) {
      if (option.equals(TTL) && options.get(option) != null) {
        hasTtl = true;
      } else if (option.equals(TTL_UNITS) && options.get(option) != null) {
        hasTtlUnits = true;
      }
    }
    return hasTtl && hasTtlUnits;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
      throws IOException {

    super.init(source, options, env);

    long ttl = Long.parseLong(options.get(TTL), 10);
    String units = options.get(TTL_UNITS);
    String startDate = options.get(START_DATE);

    if (startDate != null) {
      startDate_ = Long.parseLong(startDate, 10);
    } else {
      startDate_ = System.currentTimeMillis();
    }

    cutOffMillis_ = startDate_ - (ttl * ttlUnitsFactor(units));
  }

  @Override
  public boolean accept(Key key, Value value) {
    long timestamp = key.getTimestamp();
    return timestamp >= cutOffMillis_ && timestamp < startDate_;
  }

  private long ttlUnitsFactor(String units) {

    long ttlUnitsFactor;

    if (units.equals("DAYS")) {
      ttlUnitsFactor = (1000 * 60 * 60 * 24); // ms per day
    } else if (units.equals("HOURS")) {
      ttlUnitsFactor = (1000 * 60 * 60); // ms per hour
    } else if (units.equals("MINUTES")) {
      ttlUnitsFactor = (1000 * 60); // ms per minute
    } else if (units.equals("SECONDS")) {
      ttlUnitsFactor = (1000); // ms per second
    } else if (units.equals("MILLISECONDS")) {
      ttlUnitsFactor = 1;
    } else {
      throw new IllegalArgumentException(
          "TTL_UNITS=" + units + " must be set to a valid value. (DAYS, HOURS, MINUTES, SECONDS or MILLISECONDS)");
    }
    return ttlUnitsFactor;
  }
}
