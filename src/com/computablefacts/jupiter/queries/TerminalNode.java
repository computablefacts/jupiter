package com.computablefacts.jupiter.queries;

import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Inflectional;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Literal;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Range;
import static com.computablefacts.jupiter.queries.TerminalNode.eTermForms.Thesaurus;

import com.computablefacts.asterix.Span;
import com.computablefacts.asterix.SpanSequence;
import com.computablefacts.asterix.View;
import com.computablefacts.asterix.WildcardMatcher;
import com.computablefacts.asterix.codecs.StringCodec;
import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.datastore.DataStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Terminal (leaf) expression node class.
 * <p>
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
  public long cardinality(DataStore dataStore, Authorizations authorizations, String dataset,
      Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(
          LogFormatter.create(true).add("dataset", dataset).add("key", key_).add("value", value_).formatDebug());
    }

    if (Range.equals(form_)) {

      List<String> range = Splitter.on(QueryBuilder._TO_).trimResults().omitEmptyStrings().splitToList(value_);

      if (range.size() == 2) {

        String min = range.get(0);
        String max = range.get(1);

        boolean isValid =
            ("*".equals(min) && StringCodec.isNumber(max)) || ("*".equals(max) && StringCodec.isNumber(min)) || (
                StringCodec.isNumber(min) && StringCodec.isNumber(max));

        if (isValid) {

          // Set range
          String minTerm = "*".equals(min) ? null : min;
          String maxTerm = "*".equals(max) ? null : max;

          return dataStore.termCardinalityEstimationForBuckets(authorizations, dataset, fields(),
              minTerm == null ? null : new BigDecimal(minTerm), maxTerm == null ? null : new BigDecimal(maxTerm));
        }
      }
      return 0; // Invalid range
    }

    List<Map.Entry<String, Long>> terms = terms(dataStore, authorizations, dataset, tokenizer);

    if (terms.isEmpty()) {
      return 0;
    }
    if (Inflectional.equals(form_)) {
      return terms.stream().mapToLong(Map.Entry::getValue).sum();
    }
    if (Literal.equals(form_)) {
      return terms.stream().mapToLong(Map.Entry::getValue).max().orElse(0);
    }
    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return 0;
  }

  @Override
  public View<String> execute(DataStore dataStore, Authorizations authorizations, String dataset,
      BloomFilters<String> expectedDocsIds, Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataStore, "dataStore should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("dataset", dataset).add("key", key_).add("value", value_)
          .add("has_docs_ids", expectedDocsIds != null).add("form", form_.toString()).formatDebug());
    }

    if (Range.equals(form_)) {
      return range(dataStore, authorizations, dataset, expectedDocsIds);
    }

    List<Map.Entry<String, Long>> terms = terms(dataStore, authorizations, dataset, tokenizer);

    if (terms.isEmpty()) {
      return View.of();
    }
    if (Inflectional.equals(form_)) {
      return inflectional(dataStore, authorizations, dataset, expectedDocsIds, terms);
    }
    if (Literal.equals(form_)) {
      return literal(dataStore, authorizations, dataset, expectedDocsIds, terms);
    }
    if (Thesaurus.equals(form_)) {
      // TODO : backport code
    }
    return View.of();
  }

  private View<String> range(DataStore dataStore, Authorizations authorizations, String dataset,
      BloomFilters<String> expectedDocsIds) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    List<String> range = Splitter.on(QueryBuilder._TO_).trimResults().omitEmptyStrings().splitToList(value_);

    if (range.size() != 2) {
      return View.of(); // Invalid range
    }

    String min = range.get(0);
    String max = range.get(1);

    boolean isValid =
        ("*".equals(min) && StringCodec.isNumber(max)) || ("*".equals(max) && StringCodec.isNumber(min)) || (
            StringCodec.isNumber(min) && StringCodec.isNumber(max));

    if (!isValid) {
      return View.of(); // Invalid range
    }

    // Set range
    String minTerm = "*".equals(min) ? null : min;
    String maxTerm = "*".equals(max) ? null : max;

    return dataStore.docsIdsSorted(authorizations, dataset, fields(), minTerm == null ? null : new BigDecimal(minTerm),
        maxTerm == null ? null : new BigDecimal(maxTerm), expectedDocsIds);
  }

  private View<String> inflectional(DataStore dataStore, Authorizations authorizations, String dataset,
      BloomFilters<String> expectedDocsIds, List<Map.Entry<String, Long>> terms) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(terms, "terms should not be null");
    Preconditions.checkArgument(!terms.isEmpty(), "terms should not be empty");

    List<View<String>> docsIds = new ArrayList<>();

    for (Map.Entry<String, Long> term : terms) {
      docsIds.add(dataStore.docsIdsSorted(authorizations, dataset,
          WildcardMatcher.compact(WildcardMatcher.hasWildcards(term.getKey()) ? term.getKey() : term.getKey() + "*"),
          fields(), expectedDocsIds));
    }
    return docsIds.size() == 1 ? docsIds.get(0)
        : docsIds.get(0).mergeSorted(docsIds.subList(1, docsIds.size() - 1), String::compareTo).dedupSorted();
  }

  private View<String> literal(DataStore dataStore, Authorizations authorizations, String dataset,
      BloomFilters<String> expectedDocsIds, List<Map.Entry<String, Long>> terms) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(terms, "terms should not be null");
    Preconditions.checkArgument(!terms.isEmpty(), "terms should not be empty");

    // TODO : ensure that the order of appearance of each term is respected

    // First, fill a Bloom filter with the UUIDs of the documents. Then, filter subsequent
    // terms using the Bloom filter created with the previous term. Thus, the number of documents
    // returned should be decreasing at each iteration.
    @Var BloomFilters<String> bfs = expectedDocsIds == null ? null : new BloomFilters<>(expectedDocsIds);

    for (int i = 0; i < terms.size() - 1; i++) {

      Iterator<String> iter = dataStore.docsIdsSorted(authorizations, dataset, terms.get(i).getKey(), fields(), bfs);

      if (!iter.hasNext()) {
        return View.of();
      }

      bfs = new BloomFilters<>();

      while (iter.hasNext()) {
        bfs.put(iter.next());
      }
    }
    return dataStore.docsIdsSorted(authorizations, dataset, terms.get(terms.size() - 1).getKey(), fields(), bfs);
  }

  private Set<String> fields() {
    return Strings.isNullOrEmpty(key_) ? null : Sets.newHashSet(key_);
  }

  private List<Map.Entry<String, Long>> terms(DataStore dataStore, Authorizations authorizations, String dataset,
      Function<String, SpanSequence> tokenizer) {

    Preconditions.checkNotNull(dataStore, "dataStore should not be null");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");
    Preconditions.checkNotNull(dataset, "dataset should not be null");

    // Build a list of terms
    List<String> terms;

    if (tokenizer == null || WildcardMatcher.hasWildcards(value_)) {
      terms = Lists.newArrayList(value_);
    } else {
      terms = tokenizer.apply(value_).stream().map(Span::text).collect(Collectors.toList());
    }

    // Order terms by increasing number of matches
    return terms.stream()
        .filter(term -> !(WildcardMatcher.startsWithWildcard(term) && WildcardMatcher.endsWithWildcard(term))).map(
            term -> new AbstractMap.SimpleImmutableEntry<>(term,
                dataStore.termCardinalityEstimationForBuckets(authorizations, dataset, fields(), term)))
        .sorted(Comparator.comparingLong(AbstractMap.SimpleImmutableEntry::getValue)).collect(Collectors.toList());
  }

  public enum eTermForms {
    Inflectional, Literal, Thesaurus, Range
  }
}
