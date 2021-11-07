package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;

import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.termstore.TermStore;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public final class AccumuloTermProcessor extends AbstractTermProcessor {

  private static final Logger logger_ = LoggerFactory.getLogger(AccumuloTermProcessor.class);

  private final TermStore termStore_;
  private BatchWriter writer_;

  public AccumuloTermProcessor(TermStore termStore) {
    termStore_ = Preconditions.checkNotNull(termStore, "termStore should not be null");
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
  public boolean write(String dataset, String docId, String field, Object term,
      int nbOccurrencesInDoc) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(docId, "docId should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(term, "term should not be null");
    Preconditions.checkArgument(nbOccurrencesInDoc > 0, "nbOccurrencesInDoc must be > 0");

    List<String> path =
        Splitter.on(SEPARATOR_CURRENCY_SIGN).trimResults().omitEmptyStrings().splitToList(field);

    String vizAdm = STRING_ADM; // for backward compatibility
    String vizDataset = AbstractStorage.toVisibilityLabel(dataset + "_");
    String vizUuid = vizDataset + AbstractStorage.toVisibilityLabel(docId);

    Set<String> vizDocSpecific = Sets.newHashSet(vizUuid);
    Set<String> vizFieldSpecific = Sets.newHashSet(vizAdm);

    AbstractStorage.toVisibilityLabels(path)
        .forEach(label -> vizFieldSpecific.add(vizDataset + label));

    boolean isOk = termStore_.put(writer(), dataset, docId, field, term, nbOccurrencesInDoc,
        vizDocSpecific, vizFieldSpecific);

    if (!isOk) {
      logger_.error(LogFormatter.create(true).message("write failed").add("dataset", dataset)
          .add("doc_id", docId).add("field", field).add("term", term).formatError());
    }
    return isOk;
  }

  private BatchWriter writer() {
    if (writer_ == null) {
      writer_ = termStore_.writer();
    }
    return writer_;
  }
}
