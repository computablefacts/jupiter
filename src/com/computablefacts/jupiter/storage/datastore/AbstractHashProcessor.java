package com.computablefacts.jupiter.storage.datastore;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class AbstractHashProcessor implements AutoCloseable {

  @Override
  protected void finalize() throws Exception {
    close();
  }

  /**
   * Persist a field value as a hash.
   *
   * @param dataset the dataset.
   * @param docId   the document identifier.
   * @param field   the field name.
   * @param value   the value to hash and index.
   * @return true if the write operation succeeded, false otherwise.
   */
  public abstract boolean write(String dataset, String docId, String field, Object value);
}
