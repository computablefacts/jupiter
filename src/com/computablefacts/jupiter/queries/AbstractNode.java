/*
 * Copyright (c) 2011-2020 MNCC
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
package com.computablefacts.jupiter.queries;

import java.util.Iterator;
import java.util.Set;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.nona.types.SpanSequence;
import com.google.common.base.Function;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * Common interface for expression nodes.
 *
 * See http://www.blackbeltcoder.com/Articles/data/easy-full-text-search-queries for details.
 */
@CheckReturnValue
public abstract class AbstractNode {

  private boolean exclude_ = false;
  private boolean grouped_ = false;

  public AbstractNode() {
    super();
  }

  /**
   * Indicates this term (or both child terms) should be excluded from the results.
   */
  final public boolean exclude() {
    return this.exclude_;
  }

  final public void exclude(boolean exclude) {
    this.exclude_ = exclude;
  }

  /**
   * Indicates this term is enclosed in parentheses.
   */
  final public boolean grouped() {
    return grouped_;
  }

  final public void grouped(boolean grouped) {
    this.grouped_ = grouped;
  }

  @Deprecated
  public abstract Set<String> terms();

  public abstract long cardinality(DataStore dataStore, Scanners scanners, String dataset,
      Function<String, SpanSequence> tokenizer);

  public abstract Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilter keepDocs, Function<String, SpanSequence> tokenizer);
}
