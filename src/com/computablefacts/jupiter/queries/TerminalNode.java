package com.computablefacts.jupiter.queries;

import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Inflectional;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Literal;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Range;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Thesaurus;
import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.nona.functions.patternoperators.PatternsBackward.reverse;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.DedupIterator;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.jupiter.storage.datastore.Scanners;
import com.computablefacts.jupiter.storage.datastore.Writers;
import com.computablefacts.logfmt.LogFormatter;
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

    if (logger_.isInfoEnabled()) {
      logger_.info(LogFormatter.create(true).add("dataset", dataset).add("key", key_)
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
          Set<String> fields = Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);

          // Set range
          String minTerm = "*".equals(min) ? null : min;
          String maxTerm = "*".equals(max) ? null : max;

          return dataStore.termCardinalityEstimationForBuckets(scanners, dataset, fields,
              minTerm == null ? null : new BigDecimal(minTerm),
              maxTerm == null ? null : new BigDecimal(maxTerm));
        }
      }
      return 0; // Invalid range
    }

    List<String> terms = terms(tokenizer);

    if (terms.isEmpty()) {
      if (logger_.isWarnEnabled()) {
        logger_.warn(LogFormatter.create(true).add("dataset", dataset).add("key", key_)
            .add("value", value_).message("all terms have been discarded").formatWarn());
      }
      return 0;
    }
    if (Inflectional.equals(form_)) {
      return terms.stream().mapToLong(
          term -> dataStore.termCardinalityEstimationForBuckets(scanners, dataset, fields(), term))
          .sum();
    }
    if (Literal.equals(form_)) {
      return terms.stream().mapToLong(
          term -> dataStore.termCardinalityEstimationForBuckets(scanners, dataset, fields(), term))
          .max().orElse(0);
    }
    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return 0;
  }

  @Override
  public Iterator<String> execute(DataStore dataStore, Scanners scanners, Writers writers,
      String dataset, BloomFilters<String> docsIds, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(scanners, "scanners should not be null");
    Preconditions.checkNotNull(writers, "writers should not be null");

    if (logger_.isInfoEnabled()) {
      logger_.info(
          LogFormatter.create(true).add("dataset", dataset).add("key", key_).add("value", value_)
              .add("has_docs_ids", docsIds != null).add("form", form_.toString()).formatInfo());
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
          Set<String> fields = Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);

          // Set range
          String minTerm = "*".equals(min) ? null : min;
          String maxTerm = "*".equals(max) ? null : max;

          return dataStore.docsIds(scanners, writers, dataset, fields,
              minTerm == null ? null : new BigDecimal(minTerm),
              maxTerm == null ? null : new BigDecimal(maxTerm), null);
        }
      }
      return ITERATOR_EMPTY; // Invalid range
    }

    List<String> terms = terms(tokenizer);

    if (terms.isEmpty()) {
      if (logger_.isWarnEnabled()) {
        logger_.warn(LogFormatter.create(true).add("dataset", dataset).add("key", key_)
            .add("value", value_).message("all terms have been discarded").formatWarn());
      }
      return ITERATOR_EMPTY;
    }
    if (Inflectional.equals(form_)) {

      List<Iterator<String>> ids = new ArrayList<>();

      for (String term : terms) {
        ids.add(dataStore.docsIds(scanners, writers, dataset,
            WildcardMatcher.compact(WildcardMatcher.hasWildcards(term) ? term : term + "*"),
            fields(), docsIds));
      }
      return new DedupIterator<>(Iterators.mergeSorted(ids, String::compareTo));
    }
    if (Literal.equals(form_)) {

      // TODO : ensure that the order of appearance of each term is respected

      // First, fill a Bloom filter with the UUIDs of the documents. Then, filter subsequent
      // terms using the Bloom filter created with the previous term.
      @Var
      BloomFilters<String> bfs = docsIds == null ? null : new BloomFilters<>(docsIds);

      for (int i = 0; i < terms.size() - 1; i++) {

        Iterator<String> iter =
            dataStore.docsIds(scanners, writers, dataset, terms.get(i), fields(), bfs);

        if (!iter.hasNext()) {
          return ITERATOR_EMPTY;
        }

        bfs = new BloomFilters<>();

        while (iter.hasNext()) {
          bfs.put(iter.next());
        }
      }
      return dataStore.docsIds(scanners, writers, dataset, terms.get(terms.size() - 1), fields(),
          bfs);
    }
    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return ITERATOR_EMPTY;
  }

  private Set<String> fields() {
    return Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);
  }

  private List<String> terms(Function<String, SpanSequence> tokenizer) {

    // Sort terms by decreasing length
    ToIntFunction<String> byTermLength = term -> {
      if (WildcardMatcher.startsWithWildcard(term)) {
        return WildcardMatcher.prefix(reverse(term)).length();
      }
      return WildcardMatcher.prefix(term).length();
    };

    // Build a list of terms
    List<String> terms;

    if (tokenizer == null || WildcardMatcher.hasWildcards(value_)) {
      terms = Lists.newArrayList(value_);
    } else {
      terms = tokenizer.apply(value_).stream().map(Span::text).collect(Collectors.toList());
    }

    // Discard small terms and order the remaining ones by length
    return terms.stream().filter(term -> !(WildcardMatcher.startsWithWildcard(term)
        && WildcardMatcher.endsWithWildcard(term))).filter(term -> {
          if (WildcardMatcher.startsWithWildcard(term)) {
            return WildcardMatcher.prefix(reverse(term)).length() >= 3;
          }
          return WildcardMatcher.prefix(term).length() >= 3;
        }).sorted(Comparator.comparingInt(byTermLength).reversed()).collect(Collectors.toList());
  }

  public enum eTermForms {
    Inflectional, Literal, Thesaurus, Range
  }
}
