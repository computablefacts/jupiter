package com.computablefacts.jupiter.iterators;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;

import com.computablefacts.jupiter.storage.Constants;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreAnonymizingIterator extends AnonymizingIterator {

  public BlobStoreAnonymizingIterator() {}

  @Override
  protected AnonymizingIterator create() {
    return new BlobStoreAnonymizingIterator();
  }

  @Override
  protected void setTopKeyValue(Key key, Value value) {

    setTopKey(key);
    setTopValue(value);

    // First, ensure the user has the ADM authorization
    Set<String> auths = parsedAuths();

    if (auths.contains(Constants.STRING_ADM)) {

      VisibilityEvaluator userVizEvaluator = visibilityEvaluator(Constants.STRING_ADM);
      ColumnVisibility rowViz = key.getColumnVisibilityParsed();

      // Then, ensure the row has the ADM authorization
      if (matches(userVizEvaluator, rowViz)) {

        // Next, ensure the ADM label is the only label that allows the user to have access to the
        // row data
        auths.remove(Constants.STRING_ADM);

        if (!matches(visibilityEvaluator(auths), rowViz)) {
          setTopValue(Constants.VALUE_ANONYMIZED);
        }
      }
    }
  }
}
