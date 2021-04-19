package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.datastore.DataStore.blobStoreName;
import static com.computablefacts.jupiter.storage.datastore.DataStore.termStoreName;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class Writers implements AutoCloseable {

  private static final Logger logger_ = LoggerFactory.getLogger(Writers.class);

  private MultiTableBatchWriter writer_;
  private BatchWriter writerBlob_;
  private BatchWriter writerIndex_;

  @Deprecated
  public Writers(Configurations configurations, String name) {
    this(configurations, name, Tables.batchWriterConfig());
  }

  public Writers(Configurations configurations, String name, BatchWriterConfig config) {

    Preconditions.checkNotNull(configurations, "configurations should neither be null nor empty");
    Preconditions.checkNotNull(name, "name should neither be null nor empty");
    Preconditions.checkNotNull(config, "config should neither be null nor empty");

    try {
      writer_ = Tables.multiTableBatchWriter(configurations.connector(), config);
      writerBlob_ = writer_.getBatchWriter(blobStoreName(name));
      writerIndex_ = writer_.getBatchWriter(termStoreName(name));
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
      close();
    }
  }

  @Override
  public void close() {

    Preconditions.checkState(writer_ != null && !writer_.isClosed(), "writer is null or closed");

    try {
      writer_.close();
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    } finally {
      writer_ = null;
      writerBlob_ = null;
      writerIndex_ = null;
    }
  }

  @CanIgnoreReturnValue
  public boolean flush() {

    Preconditions.checkState(writer_ != null && !writer_.isClosed(), "writer is null or closed");

    try {
      writer_.flush();
      return true;
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    }
    return false;
  }

  public BatchWriter blob() {
    return writerBlob_;
  }

  public BatchWriter index() {
    return writerIndex_;
  }
}
