package com.computablefacts.jupiter.storage.datastore;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class AbstractBlobProcessor implements AutoCloseable {

  @Override
  protected void finalize() throws Exception {
    close();
  }

  /**
   * Persist a single JSON object.
   *
   * @param dataset the dataset.
   * @param docId   the document identifier.
   * @param blob    the JSON string.
   * @return true if the write operation succeeded, false otherwise.
   */
  public abstract boolean write(String dataset, String docId, String blob);
}
