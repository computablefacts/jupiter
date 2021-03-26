package com.computablefacts.jupiter.queries;

import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Inflectional;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Literal;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Range;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Thesaurus;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.DedupIterator;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.jupiter.storage.termstore.TermCount;
import com.computablefacts.nona.helpers.WildcardMatcher;
import com.computablefacts.nona.types.Span;
import com.computablefacts.nona.types.SpanSequence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
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
    } else if (form_ == eTermForms.Range) {
      if (!Strings.isNullOrEmpty(key_)) {
        builder.append(key_);
        builder.append(':');
      }
      builder.append('[');
      builder.append(value_);
      builder.append(']');
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

    if (Range.equals(form_)) {

      List<String> range =
          Splitter.on(QueryBuilder._TO_).trimResults().omitEmptyStrings().splitToList(value_);

      if (range.size() == 2) {

        String min = range.get(0);
        String max = range.get(1);

        boolean isValid =
            ("*".equals(min) && com.computablefacts.nona.helpers.Strings.isNumber(max))
                || ("*".equals(max) && com.computablefacts.nona.helpers.Strings.isNumber(min))
                || (com.computablefacts.nona.helpers.Strings.isNumber(min)
                    && com.computablefacts.nona.helpers.Strings.isNumber(max));

        if (isValid) {

          // Set fields
          Set<String> keepFields = Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);

          // Set range
          String minTerm = "*".equals(min) ? null : min;
          String maxTerm = "*".equals(max) ? null : max;

          @Var
          long card = 0;
          Iterator<Pair<String, List<TermCount>>> iter =
              dataStore.numericalRangeCount(scanners, dataset, minTerm, maxTerm, keepFields);

          while (iter.hasNext()) {
            Pair<String, List<TermCount>> pair = iter.next();
            card += pair.getSecond().stream().mapToLong(TermCount::count).sum();
          }
          return card;
        }
      }
      return 0; // Invalid range
    }

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

        Iterator<Pair<String, List<TermCount>>> iter = dataStore.termCount(scanners, dataset,
            WildcardMatcher.compact(WildcardMatcher.hasWildcards(term) ? term : term + "*"));

        while (iter.hasNext()) {
          Pair<String, List<TermCount>> pair = iter.next();
          card += pair.getSecond().stream().mapToLong(TermCount::count).sum();
        }
      }
      return card;
    }

    if (Literal.equals(form_)) {

      @Var
      long card = 0;

      for (String term : terms) {

        Iterator<Pair<String, List<TermCount>>> iter = dataStore.termCount(scanners, dataset, term);

        @Var
        long sum = 0;

        while (iter.hasNext()) {
          Pair<String, List<TermCount>> pair = iter.next();
          sum += pair.getSecond().stream().mapToLong(TermCount::count).sum();
        }

        card = Math.max(card, sum);
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
      String dataset, BloomFilters<String> keepDocs, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset),
        "dataset should neither be null nor empty");

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatterManager.logFormatter().add("dataset", dataset).add("key", key_)
          .add("value", value_).add("hasKeepDocs", keepDocs != null).add("form", form_.toString())
          .formatInfo());
    }

    if (Range.equals(form_)) {

      List<String> range =
          Splitter.on(QueryBuilder._TO_).trimResults().omitEmptyStrings().splitToList(value_);

      if (range.size() == 2) {

        String min = range.get(0);
        String max = range.get(1);

        boolean isValid =
            ("*".equals(min) && com.computablefacts.nona.helpers.Strings.isNumber(max))
                || ("*".equals(max) && com.computablefacts.nona.helpers.Strings.isNumber(min))
                || (com.computablefacts.nona.helpers.Strings.isNumber(min)
                    && com.computablefacts.nona.helpers.Strings.isNumber(max));

        if (isValid) {

          // Set fields
          Set<String> keepFields = Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);

          // Set range
          String minTerm = "*".equals(min) ? null : min;
          String maxTerm = "*".equals(max) ? null : max;

          return dataStore.searchByNumericalRange(scanners, writers, dataset, minTerm, maxTerm,
              keepFields, null);
        }
      }
      return Constants.ITERATOR_EMPTY; // Invalid range
    }

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
    Inflectional, Literal, Thesaurus, Range
  }
}
