/**
 * Copyright (c) 2011-2018 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
package com.computablefacts.jupiter.filters;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.nona.helpers.WildcardMatcher;

public class WildcardFilter extends Filter {

  private static final String COLUMN_CRITERION = "col";
  private static final String WILDCARD_CRITERION = "wc-";
  private static final String AND_CRITERION = "and";

  private final Set<String> wildcards_ = new HashSet<>();
  private String column_;
  private boolean and_ = false;

  public static void applyOnRow(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "ROW");
  }

  public static void applyOnColumnFamily(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "CF");
  }

  public static void applyOnColumnQualifier(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "CQ");
  }

  public static void applyOnValue(IteratorSetting setting) {
    setting.addOption(COLUMN_CRITERION, "VALUE");
  }

  public static void addWildcard(IteratorSetting setting, String pattern) {
    if (pattern != null) {
      setting.addOption(WILDCARD_CRITERION + setting.getOptions().size(), pattern);
    }
  }

  public static void setAnd(IteratorSetting setting) {
    setting.addOption(AND_CRITERION, Boolean.toString(true));
  }

  public static void setOr(IteratorSetting setting) {
    setting.addOption(AND_CRITERION, Boolean.toString(false));
  }

  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(COLUMN_CRITERION, "Column on which the filter must be applied.");
    options.put(WILDCARD_CRITERION, "Set of wildcards.");
    options.put(AND_CRITERION, "Apply an AND operator between the wildcards.");

    return new IteratorOptions("WildcardFilter",
        "WildcardFilter accepts or rejects each Key/Value pair based on a given column evaluated against a given list of patterns.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 2) {
      return false;
    }
    for (String option : options.keySet()) {
      if (!option.startsWith(COLUMN_CRITERION) && !option.startsWith(WILDCARD_CRITERION)
          && !option.equals(AND_CRITERION)) {
        return false;
      }
      if (option.startsWith(WILDCARD_CRITERION) && options.get(option) == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    wildcards_
        .addAll(options.keySet().stream().filter(option -> option.startsWith(WILDCARD_CRITERION))
            .map(options::get).collect(Collectors.toList()));
    column_ = options.getOrDefault(COLUMN_CRITERION, "VALUE");
    and_ = Boolean.parseBoolean(options.getOrDefault(AND_CRITERION, "false"));
  }

  @Override
  public boolean accept(Key key, Value value) {

    String val;

    if ("ROW".equals(column_)) {
      val = key == null || key.getRow() == null ? null : key.getRow().toString();
    } else if ("CF".equals(column_)) {
      val = key == null || key.getColumnFamily() == null ? null : key.getColumnFamily().toString();
    } else if ("CQ".equals(column_)) {
      val = key == null || key.getColumnQualifier() == null ? null
          : key.getColumnQualifier().toString();
    } else { // VALUE
      val = value == null ? null : value.toString();
    }

    if (val == null) {
      return true;
    }

    for (String wildcard : wildcards_) {

      boolean match = WildcardMatcher.match(val, wildcard);

      if (and_ && !match) {
        return false;
      }
      if (!and_ && match) {
        return true;
      }
    }
    return and_;
  }
}
