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
    try {
      if (writer_ != null) {
        writer_.close();
      }
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    } finally {
      writer_ = null;
      writerBlob_ = null;
      writerIndex_ = null;
    }
  }

  public void flush() {
    try {
      writer_.flush();
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    }
  }

  public BatchWriter blob() {
    return writerBlob_;
  }

  public BatchWriter index() {
    return writerIndex_;
  }
}
