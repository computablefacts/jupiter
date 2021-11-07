package com.computablefacts.jupiter.storage.datastore;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.iterators.MaskingIterator;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class FileHashProcessor extends AbstractHashProcessor {

  private static final Logger logger_ = LoggerFactory.getLogger(FileHashProcessor.class);

  private final String filename_;
  private BufferedWriter writer_;

  public FileHashProcessor(String filename) {
    filename_ = Preconditions.checkNotNull(filename, "filename should neither be null nor empty");
  }

  @Override
  public void close() {
    if (writer_ != null) {
      try {
        writer_.close();
      } catch (IOException e) {
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

    Preconditions.checkNotNull(dataset, "dataset should neither be null nor empty");
    Preconditions.checkNotNull(field, "field should neither be null nor empty");
    Preconditions.checkNotNull(value, "value should neither be null nor empty");
    Preconditions.checkNotNull(docId, "docId should neither be null nor empty");

    BufferedWriter writer = writer();

    Preconditions.checkState(writer != null, "writer should not be null");

    String hash = MaskingIterator.hash(null, value);

    try {
      writer.write(dataset);
      writer.write('\t');
      writer.write(field);
      writer.write('\t');
      writer.write(hash);
      writer.write('\t');
      writer.write(docId);
      writer.newLine();
    } catch (IOException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return true;
  }

  private BufferedWriter writer() {
    if (writer_ == null) {
      try {
        writer_ = new BufferedWriter(new OutputStreamWriter(
            new GZIPOutputStream(new FileOutputStream(filename_)), StandardCharsets.UTF_8));
      } catch (IOException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return writer_;
  }
}
