package com.computablefacts.jupiter.storage.datastore;

import com.computablefacts.asterix.View;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class AbstractHashProcessor implements AutoCloseable {

  /**
   * Persist a field value as a hash.
   *
   * @param dataset the dataset.
   * @param docId the document identifier.
   * @param field the field name.
   * @param value the value to hash and index.
   * @return true if the write operation succeeded, false otherwise.
   */
  public abstract boolean write(String dataset, String docId, String field, Object value);

  /**
   * Get documents ids (sorted).
   *
   * @param dataset dataset.
   * @param field the field.
   * @param hash the field hashed value.
   * @return an unordered set of docs ids.
   */
  public abstract View<String> readSorted(String dataset, String field, String hash);

  /**
   * Get documents ids (unsorted).
   *
   * @param dataset dataset.
   * @param field the field.
   * @param hash the field hashed value.
   * @return an unordered set of docs ids.
   */
  public abstract View<String> read(String dataset, String field, String hash);

  /**
   * Cache fields and values.
   *
   * @param dataset dataset.
   * @param field the field.
   * @param value the field value.
   * @param docId the document id.
   */
  protected abstract boolean write(String dataset, String field, String value, String docId);
}
