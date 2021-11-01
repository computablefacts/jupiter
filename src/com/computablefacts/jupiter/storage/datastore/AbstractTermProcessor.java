package com.computablefacts.jupiter.storage.datastore;

import java.util.Set;

import com.computablefacts.asterix.View;
import com.computablefacts.jupiter.BloomFilters;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class AbstractTermProcessor implements AutoCloseable {

  /**
   * Persist a single term.
   *
   * @param dataset the dataset.
   * @param docId the document identifier.
   * @param field the field name.
   * @param term the term to index.
   * @param nbOccurrencesInDoc the number of occurrences of the term in the document.
   * @return true if the write operation succeeded, false otherwise.
   */
  public abstract boolean write(String dataset, String docId, String field, Object term,
      int nbOccurrencesInDoc);

  /**
   * Get the ids of all documents where at least one token matches "term".
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param term searched term. Might contain wildcard characters.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public abstract View<String> read(String dataset, String term, Set<String> fields,
      BloomFilters<String> docsIds);

  /**
   * Get the ids of all documents where at least one token matches a term in [minTerm, maxTerm].
   *
   * @param dataset dataset (optional).
   * @param fields which fields must be considered (optional).
   * @param minTerm first searched term (included). Wildcard characters are not allowed.
   * @param maxTerm last searched term (excluded). Wildcard characters are not allowed.
   * @param docsIds which docs must be considered (optional).
   * @return an ordered stream of documents ids.
   */
  public abstract View<String> read(String dataset, Set<String> fields, Object minTerm,
      Object maxTerm, BloomFilters<String> docsIds);
}
