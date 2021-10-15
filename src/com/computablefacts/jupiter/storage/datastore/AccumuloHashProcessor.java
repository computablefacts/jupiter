package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.ITERATOR_EMPTY;
import static com.computablefacts.jupiter.storage.Constants.NB_QUERY_THREADS;
import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.FlattenIterator;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class AccumuloHashProcessor extends AbstractHashProcessor {

  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloHashProcessor.class);

  private final BlobStore blobStore_;
  private final Authorizations authorizations_;
  private BatchWriter writer_;
  private ScannerBase reader_;

  public AccumuloHashProcessor(BlobStore blobStore, Authorizations authorizations) {
    blobStore_ =
        Preconditions.checkNotNull(blobStore, "blobStore should neither be null nor empty");
    authorizations_ = authorizations == null ? Authorizations.EMPTY : authorizations;
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
    if (reader_ != null) {
      reader_.close();
      reader_ = null;
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

    ScannerBase scanner = scanner();
    scanner.clearColumns();
    scanner.clearScanIterators();

    if (field != null) {
      BlobStore.allArrayShards().forEach(cf -> scanner.fetchColumn(new Text(cf), new Text(field)));
    } else {
      BlobStore.allArrayShards().forEach(cf -> scanner.fetchColumnFamily(new Text(cf)));
    }

    Set<Range> ranges = new HashSet<>();

    if (hash != null && field != null) {
      BlobStore.allArrayShards()
          .forEach(cf -> ranges.add(Range.exact(dataset + SEPARATOR_NUL + hash, cf, field)));
    } else if (hash != null) {
      BlobStore.allArrayShards()
          .forEach(cf -> ranges.add(Range.exact(dataset + SEPARATOR_NUL + hash, cf)));
    } else {
      ranges.add(Range.prefix(dataset + SEPARATOR_NUL));
    }

    if (!AbstractStorage.setRanges(scanner, ranges)) {
      return ITERATOR_EMPTY;
    }
    return new FlattenIterator<>(scanner.iterator(), entry -> {
      Value val = entry.getValue();
      return Splitter.on(SEPARATOR_NUL).trimResults().omitEmptyStrings()
          .splitToList(val.toString());
    });
  }

  @Override
  protected boolean write(String dataset, String field, String value, String docId) {

    Preconditions.checkNotNull(field, "field should neither be null nor empty");
    Preconditions.checkNotNull(value, "value should neither be null nor empty");
    Preconditions.checkNotNull(docId, "docId should neither be null nor empty");

    String hash = MaskingIterator.hash(null, value);
    Mutation mutation = new Mutation(dataset + SEPARATOR_NUL + hash);
    mutation.put(BlobStore.arrayShard(docId), field, docId);

    try {
      writer().addMutation(mutation);
      return true;
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return false;
  }

  public BatchWriter writer() {
    if (writer_ == null) {
      writer_ = blobStore_.writer();
    }
    return writer_;
  }

  public ScannerBase scanner() {
    if (reader_ == null) {
      reader_ = blobStore_.batchScanner(authorizations_, NB_QUERY_THREADS);
    }
    return reader_;
  }
}
