package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.STRING_RAW_DATA;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.blobstore.BlobStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class AccumuloBlobProcessor extends AbstractBlobProcessor {

  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloBlobProcessor.class);

  private final BlobStore blobStore_;
  private BatchWriter writer_;

  AccumuloBlobProcessor(BlobStore blobStore) {
    blobStore_ = Preconditions.checkNotNull(blobStore, "blobStore should not be null");
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
  public boolean write(String dataset, String docId, String blob) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(blob, "blob should not be null");

    String vizAdm = STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(docId);
    String vizRawData = vizDataset + STRING_RAW_DATA;

    boolean isOk = blobStore_.putJson(writer(), dataset, docId,
        Sets.newHashSet(vizAdm, vizUuid, vizRawData), blob);

    if (!isOk) {
      logger_.error(LogFormatter.create(true).message("write failed").add("dataset", dataset)
          .add("doc_id", docId).add("blob", blob).formatError());
      return false;
    }
    return true;
  }

  private BatchWriter writer() {
    if (writer_ == null) {
      writer_ = blobStore_.writer();
    }
    return writer_;
  }
}
