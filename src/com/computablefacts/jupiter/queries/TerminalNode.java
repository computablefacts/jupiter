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

import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Inflectional;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Literal;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Thesaurus;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.DedupIterator;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.jupiter.storage.termstore.TermCard;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.computablefacts.nona.types.Span;
import com.computablefacts.nona.types.SpanSequence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * Terminal (leaf) expression node class.
 *
 * See http://www.blackbeltcoder.com/Articles/data/easy-full-text-search-queries for details.
 */
@CheckReturnValue
final public class TerminalNode extends AbstractNode {

  private static final Logger logger_ = LoggerFactory.getLogger(TerminalNode.class);

  private final eTermForms form_;
  private final String key_;
  private final String value_;

  public TerminalNode(eTermForms form, String key, String value) {
    form_ = Preconditions.checkNotNull(form, "form should not be null");
    key_ = Strings.nullToEmpty(key);
    value_ = Strings.nullToEmpty(value);
  }

  @Override
  public String toString() {

    // TODO : deal with subject

    StringBuilder builder = new StringBuilder();

    if (form_ == Inflectional) {
      if (!Strings.isNullOrEmpty(key_)) {
        builder.append(key_);
        builder.append(':');
      }
      builder.append(value_);
    } else if (form_ == eTermForms.Literal) {
      if (!Strings.isNullOrEmpty(key_)) {
        builder.append(key_);
        builder.append(':');
      }
      builder.append('\"');
      builder.append(value_);
      builder.append('\"');
    } else if (form_ == eTermForms.Thesaurus) {
      if (!Strings.isNullOrEmpty(key_)) {
        builder.append(key_);
        builder.append(':');
      }
      builder.append('~');
      builder.append(value_);
    }
    return (exclude() ? "Not(" : "") + builder + (exclude() ? ")" : "");
  }

  public eTermForms form() {
    return form_;
  }

  public String key() {
    return key_;
  }

  public String value() {
    return value_;
  }

  @Override
  public Set<String> terms() {
    return Sets.newHashSet(value_);
  }

  @Override
  public long cardinality(DataStore dataStore, Scanners scanners, String dataset,
      Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset),
        "dataset should neither be null nor empty");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).add("key", key_)
          .add("value", value_).formatInfo());
    }

    // TODO : backport NOT implementation

    // Tokenize object
    @Var
    List<String> terms;

    if (tokenizer == null || WildcardMatcher.hasWildcards(value_)) {
      terms = Lists.newArrayList(value_);
    } else {
      terms = tokenizer.apply(value_).stream().map(Span::text).collect(Collectors.toList());
    }

    // Discard small terms
    terms = terms.stream().filter(term -> term.length() >= 3).collect(Collectors.toList());

    if (terms.isEmpty()) {
      if (logger_.isWarnEnabled()) {
        logger_.warn(LogFormatterManager.logFormatter().add("dataset", dataset).add("key", key_)
            .add("value", value_).message("all terms have been discarded").formatWarn());
      }
      return 0;
    }

    // Execute query according to the query form
    if (Inflectional.equals(form_)) {

      @Var
      long card = 0;

      for (String term : terms) {

        Iterator<Pair<String, List<TermCard>>> iter = dataStore.termCard(scanners, dataset,
            WildcardMatcher.compact(WildcardMatcher.hasWildcards(term) ? term : term + "*"));

        while (iter.hasNext()) {
          card += iter.next().getSecond().stream().mapToLong(TermCard::cardinality).sum();
        }
      }
      return card;
    }

    if (Literal.equals(form_)) {

      @Var
      long card = Long.MAX_VALUE;

      for (String term : terms) {

        Iterator<Pair<String, List<TermCard>>> iter = dataStore.termCard(scanners, dataset, term);

        @Var
        long sum = 0;

        while (iter.hasNext()) {
          sum = Math.max(sum,
              iter.next().getSecond().stream().mapToLong(TermCard::cardinality).sum());
        }

        card = Math.min(card, sum);

        if (card == 0) {
          return 0;
        }
      }
      return card;
    }

    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return 0;
  }

  @Override
  public Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilter keepDocs, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset),
        "dataset should neither be null nor empty");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).add("key", key_)
          .add("value", value_).add("hasKeepDocs", keepDocs != null).formatInfo());
    }

    // TODO : backport NOT implementation

    // Tokenize object
    @Var
    List<String> terms;

    if (tokenizer == null || WildcardMatcher.hasWildcards(value_)) {
      terms = Lists.newArrayList(value_);
    } else {
      terms = tokenizer.apply(value_).stream().map(Span::text).collect(Collectors.toList());
    }

    // Discard small terms
    terms = terms.stream().filter(term -> term.length() >= 3).collect(Collectors.toList());

    if (terms.isEmpty()) {
      return Constants.ITERATOR_EMPTY;
    }

    // Set fields
    Set<String> keepFields = Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);

    // Execute query according to the query form
    if (Inflectional.equals(form_)) {

      List<Iterator<String>> ids = new ArrayList<>();

      for (String term : terms) {
        ids.add(dataStore.searchByTerm(scanners, writers, dataset,
            WildcardMatcher.compact(WildcardMatcher.hasWildcards(term) ? term : term + "*"),
            keepFields));
      }
      return new DedupIterator<>(Iterators.mergeSorted(ids, String::compareTo));
    }

    if (Literal.equals(form_)) {
      return dataStore.searchByTerms(scanners, writers, dataset, terms, keepFields);
    }

    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return Constants.ITERATOR_EMPTY;
  }

  public enum eTermForms {
    Inflectional, Literal, Thesaurus
  }
}
