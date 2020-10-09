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
