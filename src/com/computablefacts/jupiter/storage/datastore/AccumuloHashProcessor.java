package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;
import static com.computablefacts.jupiter.storage.Constants.VALUE_EMPTY;

import java.util.Date;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class AccumuloHashProcessor extends AbstractHashProcessor {

  static final String CF = "H";
  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloHashProcessor.class);

  private final TermStore termStore_;
  private BatchWriter writer_;

  AccumuloHashProcessor(TermStore termStore) {
    termStore_ =
        Preconditions.checkNotNull(termStore, "termStore should neither be null nor empty");
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

    if (value instanceof Date) {
      return write(dataset, field, ((Date) value).toInstant().toString(), docId);
    }
    return write(dataset, field, value.toString(), docId);
  }

  private boolean write(String dataset, String field, String value, String docId) {

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
}
