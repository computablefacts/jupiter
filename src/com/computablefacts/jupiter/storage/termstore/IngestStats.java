package com.computablefacts.jupiter.storage.termstore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Table;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class IngestStats implements AutoCloseable {

  private static final Logger logger_ = LoggerFactory.getLogger(IngestStats.class);

  private final AbstractStorage storage_;
  private final BatchWriter writer_;

  // <dataset, <field count>>
  private final Map<String, Multiset<String>> count_ = new HashMap<>();

  // <dataset, <field card>>
  private final Map<String, Multiset<String>> card_ = new HashMap<>();

  // <dataset, field, labels>
  private final Table<String, String, Set<String>> viz_ = HashBasedTable.create();

  public IngestStats(AbstractStorage storage, BatchWriter writer) {
    storage_ = Preconditions.checkNotNull(storage, "storage should not be null");
    writer_ = Preconditions.checkNotNull(writer, "writer should not be null");
  }

  @Override
  public void close() {
    flush();
  }

  public void count(String dataset, String field, int count) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (!count_.containsKey(dataset)) {
      count_.put(dataset, HashMultiset.create());
    }
    count_.get(dataset).add(field, count);
  }

  public void card(String dataset, String field, int card) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");

    if (!card_.containsKey(dataset)) {
      card_.put(dataset, HashMultiset.create());
    }
    card_.get(dataset).add(field, card);
  }

  public void visibility(String dataset, String field, Set<String> labels) {

    Preconditions.checkNotNull(dataset, "dataset should not be null");
    Preconditions.checkNotNull(field, "field should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");

    if (!viz_.contains(dataset, field)) {
      viz_.put(dataset, field, new HashSet<>());
    }
    viz_.get(dataset, field).addAll(labels);
  }

  public void flush() {

    if (storage_ == null || writer_ == null) {
      return;
    }

    for (Table.Cell<String, String, Set<String>> cell : viz_.cellSet()) {

      String ds = TermStore.visibility(cell.getRowKey());
      Text dataset = new Text(ds);
      Text field = new Text(cell.getColumnKey());
      Value viz = new Value(Joiner.on(Constants.SEPARATOR_NUL).join(cell.getValue()));
      ColumnVisibility cv =
          new ColumnVisibility(Constants.STRING_ADM + "|" + AbstractStorage.toVisibilityLabel(ds));

      if (!storage_.add(writer_, field, dataset, null, cv, viz)) {
        logger_.error(LogFormatterManager.logFormatter().message("VIZ : an error occurred")
            .add("dataset", dataset).add("field", field).add("viz", viz).formatError());
      }
    }

    viz_.clear();

    for (String ds : count_.keySet()) {

      String cnt = TermStore.count(ds);
      Text dataset = new Text(cnt);

      for (Multiset.Entry<String> entry : count_.get(ds).entrySet()) {

        Text field = new Text(entry.getElement());
        Value count = new Value(Long.toString(entry.getCount(), 10));
        ColumnVisibility cv = new ColumnVisibility(
            Constants.STRING_ADM + "|" + AbstractStorage.toVisibilityLabel(cnt));

        if (!storage_.add(writer_, field, dataset, null, cv, count)) {
          logger_.error(LogFormatterManager.logFormatter().message("CNT : an error occurred")
              .add("dataset", dataset).add("field", field).add("count", count).formatError());
        }
      }
    }

    count_.clear();

    for (String ds : card_.keySet()) {

      String cnt = TermStore.card(ds);
      Text dataset = new Text(cnt);

      for (Multiset.Entry<String> entry : card_.get(ds).entrySet()) {

        Text field = new Text(entry.getElement());
        Value card = new Value(Long.toString(entry.getCount(), 10));
        ColumnVisibility cv = new ColumnVisibility(
            Constants.STRING_ADM + "|" + AbstractStorage.toVisibilityLabel(cnt));

        if (!storage_.add(writer_, field, dataset, null, cv, card)) {
          logger_.error(LogFormatterManager.logFormatter().message("CARD : an error occurred")
              .add("dataset", dataset).add("field", field).add("card", card).formatError());
        }
      }
    }

    card_.clear();

    try {
      writer_.flush();
    } catch (MutationsRejectedException e) {
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
    }
  }
}
