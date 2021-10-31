package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.filters.TermStoreBucketFieldFilter;
import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class AccumuloHashProcessor extends AbstractHashProcessor {

  private static final String CF = "H";
  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloHashProcessor.class);

  private final TermStore termStore_;
  private final Authorizations authorizations_;
  private final int nbQueryThreads_;
  private BatchWriter writer_;

  AccumuloHashProcessor(TermStore termStore, Authorizations authorizations, int nbQueryThreads) {
    termStore_ =
        Preconditions.checkNotNull(termStore, "termStore should neither be null nor empty");
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
  public boolean write(String dataset, String docId, String field, Object value) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(value, "value should not be null");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("dataset", dataset).add("doc_id", docId)
          .add("field", field).add("value", value).formatDebug());
    }

    if (value instanceof Date) {
      return write(dataset, field, ((Date) value).toInstant().toString(), docId);
    }
    return write(dataset, field, value.toString(), docId);
  }

  @Override
  public Iterator<String> read(String dataset, String field, String hash) {

    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");

    if (logger_.isDebugEnabled()) {
      logger_.debug(LogFormatter.create(true).add("dataset", dataset).add("field", field)
          .add("hash", hash).formatDebug());
    }

    Set<String> docsIds = new HashSet<>();

    try (ScannerBase scanner = scanner()) {

      scanner.clearColumns();
      scanner.clearScanIterators();
      scanner.fetchColumnFamily(new Text(CF));

      Range range;

      if (hash != null) {
        range = Range.exact(dataset + SEPARATOR_NUL + hash, CF);
      } else {
        range = Range.prefix(dataset + SEPARATOR_NUL);
      }

      if (!AbstractStorage.setRanges(scanner, Sets.newHashSet(range))) {
        return ITERATOR_EMPTY;
      }

      if (field != null) {

        IteratorSetting setting =
            new IteratorSetting(31, "TermStoreBucketFieldFilter", TermStoreBucketFieldFilter.class);
        TermStoreBucketFieldFilter.setFieldsToKeep(setting, Sets.newHashSet(field));

        scanner.addScanIterator(setting);
      }

      Iterators.transform(scanner.iterator(), e -> {
        String cq = e.getKey().getColumnQualifier().toString();
        return cq.substring(0, cq.indexOf(SEPARATOR_NUL));
      }).forEachRemaining(docsIds::add);
    }
    return docsIds.iterator();
  }

  @Override
  protected boolean write(String dataset, String field, String value, String docId) {

    Preconditions.checkNotNull(field, "field should neither be null nor empty");
    Preconditions.checkNotNull(value, "value should neither be null nor empty");
    Preconditions.checkNotNull(docId, "docId should neither be null nor empty");

    String hash = MaskingIterator.hash(null, value);
    Mutation mutation = new Mutation(dataset + SEPARATOR_NUL + hash);
    mutation.put(CF, docId + SEPARATOR_NUL + field, VALUE_EMPTY);

    try {
      writer().addMutation(mutation);
      return true;
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  private BatchWriter writer() {
    if (writer_ == null) {
      writer_ = termStore_.writer();
    }
    return writer_;
  }

  private ScannerBase scanner() {
    if (nbQueryThreads_ == 1) {
      return termStore_.scanner(authorizations_);
    }
    return termStore_.batchScanner(authorizations_, nbQueryThreads_);
  }
}
