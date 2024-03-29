package com.computablefacts.jupiter.storage.datastore;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class AbstractTermProcessor implements AutoCloseable {

  @Override
  protected void finalize() throws Exception {
    close();
  }

  /**
   * Persist a single term.
   *
   * @param dataset            the dataset.
   * @param docId              the document identifier.
   * @param field              the field name.
   * @param term               the term to index.
   * @param nbOccurrencesInDoc the number of occurrences of the term in the document.
   * @return true if the write operation succeeded, false otherwise.
   */
  public abstract boolean write(String dataset, String docId, String field, Object term, int nbOccurrencesInDoc);
}
