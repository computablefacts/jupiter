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

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class TermStoreDocFieldFilter extends Filter {

  private static final String DOCS_CRITERION = "d";
  private static final String FIELDS_CRITERION = "f";

  private BloomFilters<String> keepDocs_;
  private Set<String> keepFields_;

  public static void setDocsToKeep(IteratorSetting setting, BloomFilters<String> bf) {
    if (bf != null) {
      setting.addOption(DOCS_CRITERION, BloomFilters.toString(bf));
    }
  }

  public static void setFieldsToKeep(IteratorSetting setting, Set<String> fields) {
    if (fields != null) {
      setting.addOption(FIELDS_CRITERION, Joiner.on(SEPARATOR_NUL).join(fields));
    }
  }

  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put(DOCS_CRITERION, "Documents to keep.");
    options.put(FIELDS_CRITERION, "Fields patterns to keep.");

    return new IteratorOptions("TermStoreDocFieldFilter",
        "TermStoreDocFieldFilter accepts or rejects each Key/Value pair based on documents and/or fields filters.",
        options, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.size() < 1 || options.size() > 2) {
      return false;
    }
    if (options.containsKey(DOCS_CRITERION)) {
      if (options.get(DOCS_CRITERION) == null) {
        return false;
      }
    }
    if (options.containsKey(FIELDS_CRITERION)) {
      return options.get(FIELDS_CRITERION) != null;
    }
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    super.init(source, options, env);

    keepDocs_ =
        options.containsKey(DOCS_CRITERION) ? BloomFilters.fromString(options.get(DOCS_CRITERION))
            : null;
    keepFields_ = options.containsKey(FIELDS_CRITERION)
        ? Sets.newHashSet(Splitter.on(SEPARATOR_NUL).split(options.get(FIELDS_CRITERION)))
        : null;
  }

  @Override
  public boolean accept(Key key, Value value) {

    if (key == null || key.getColumnQualifier() == null) {
      return false;
    }

    String cq = key.getColumnQualifier().toString();
    int index = cq.indexOf(SEPARATOR_NUL);
    String doc = cq.substring(0, index);
    String field = cq.substring(index + 1);

    if (keepDocs_ != null) {
      if (!acceptDoc(doc)) {
        return false;
      }
    }
    if (keepFields_ != null) {
      if (!acceptField(field)) {
        return false;
      }
    }
    return true;
  }

  private boolean acceptField(String field) {
    for (String pattern : keepFields_) {
      if (WildcardMatcher.match(field, pattern)) {
        return true;
      }
    }
    return false;
  }

  private boolean acceptDoc(String doc) {
    return keepDocs_.mightContain(doc);
  }
}
