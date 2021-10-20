package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.BloomFilters;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public final class AccumuloTermProcessor extends AbstractTermProcessor {

  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloTermProcessor.class);

  private final TermStore termStore_;
  private final Authorizations authorizations_;
  private final int nbQueryThreads_;
  private BatchWriter writer_;

  AccumuloTermProcessor(TermStore termStore, Authorizations authorizations, int nbQueryThreads) {
    termStore_ = Preconditions.checkNotNull(termStore, "termStore should not be null");
    authorizations_ = authorizations == null ? Authorizations.EMPTY : authorizations;
    nbQueryThreads_ = nbQueryThreads <= 0 ? 1 : nbQueryThreads;
  }

  @Override
  public void close() {
    if (writer_ != null) {
      try {
        writer_.close();
      } catch (MutationsRejectedException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
      writer_ = null;
    }
  }

  @Override
  public boolean write(String dataset, String docId, String field, Object term,
      int nbOccurrencesInDoc) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrencesInDoc > 0, "nbOccurrencesInDoc must be > 0");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("namespace", termStore_.tableName())
          .add("dataset", dataset).add("doc_id", docId).add("field", field).add("term", term)
          .add("nb_occurrences_in_doc", nbOccurrencesInDoc).formatDebug());
    }

    List<String> path =
        Splitter.on(SEPARATOR_CURRENCY_SIGN).trimResults().omitEmptyStrings().splitToList(field);

    String vizAdm = STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(docId);

    Set<String> vizDocSpecific = Sets.newHashSet(vizUuid);
    Set<String> vizFieldSpecific = Sets.newHashSet(vizAdm);

    AbstractStorage.toVisibilityLabels(path)
        .forEach(label -> vizFieldSpecific.add(vizDataset + label));

    boolean isOk = termStore_.put(writer(), dataset, docId, field, term, nbOccurrencesInDoc,
        vizDocSpecific, vizFieldSpecific);

    if (!isOk) {
      logger_.error(LogFormatter.create(true).message("write failed").add("dataset", dataset)
          .add("doc_id", docId).add("field", field).add("term", term).formatError());
    }
    return isOk;
  }

  @Override
  public Iterator<String> read(String dataset, String term, Set<String> fields,
      BloomFilters<String> docsIds) {

    Preconditions.checkNotNull(term, "term should not be null");

    // Extract buckets ids, i.e. documents ids, from the TermStore
    // TODO : load docs on-demand to prevent segfault
    Set<String> bucketsIds = new HashSet<>();

    try (ScannerBase scanner = scanner()) {
      Iterators.transform(termStore_.bucketsIds(scanner, dataset, fields, term, docsIds),
          t -> t.bucketId() + SEPARATOR_NUL + t.dataset()).forEachRemaining(bucketsIds::add);
    }

    // Returns an iterator over the documents ids
    return bucketsIds.stream().sorted().distinct().iterator();
  }

  @Override
  public Iterator<String> read(String dataset, Set<String> fields, Object minTerm, Object maxTerm,
      BloomFilters<String> docsIds) {

    Preconditions.checkArgument(minTerm != null || maxTerm != null,
        "minTerm and maxTerm cannot be null at the same time");
    Preconditions.checkArgument(
        minTerm == null || maxTerm == null || minTerm.getClass().equals(maxTerm.getClass()),
        "minTerm and maxTerm must be of the same type");

    // Extract buckets ids, i.e. documents ids, from the TermStore
    // TODO : load docs on-demand to prevent segfault
    Set<String> bucketsIds = new HashSet<>();

    try (ScannerBase scanner = scanner()) {
      Iterators
          .transform(termStore_.bucketsIds(scanner, dataset, fields, minTerm, maxTerm, docsIds),
              t -> t.bucketId() + SEPARATOR_NUL + t.dataset())
          .forEachRemaining(bucketsIds::add);
    }

    // Returns an iterator over the documents ids
    return bucketsIds.stream().sorted().distinct().iterator();
  }

  BatchWriter writer() {
    if (writer_ == null) {
      writer_ = termStore_.writer();
    }
    return writer_;
  }

  ScannerBase scanner() {
    if (nbQueryThreads_ == 1) {
      return termStore_.scanner(authorizations_);
    }
    return termStore_.batchScanner(authorizations_, nbQueryThreads_);
  }
}
