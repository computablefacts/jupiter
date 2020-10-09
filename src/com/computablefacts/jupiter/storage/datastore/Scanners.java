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
package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.datastore.DataStore.blobStoreName;
import static com.computablefacts.jupiter.storage.datastore.DataStore.termStoreName;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.jupiter.Tables;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
public class Scanners implements AutoCloseable {

  private ScannerBase scannerBlob_;
  private ScannerBase scannerIndex_;

  @Deprecated
  public Scanners(Configurations configurations, String name, Authorizations authorizations) {
    this(configurations, name, authorizations, 1);
  }

  public Scanners(Configurations configurations, String name, @Var Authorizations authorizations,
      int nbQueryThreads) {

    Preconditions.checkNotNull(configurations, "configurations should neither be null nor empty");
    Preconditions.checkNotNull(name, "name should neither be null nor empty");

    Connector connector = configurations.connector();
    authorizations = authorizations == null ? Authorizations.EMPTY : authorizations;

    if (nbQueryThreads <= 1) {
      scannerBlob_ = Tables.scanner(connector, blobStoreName(name), authorizations);
      scannerIndex_ = Tables.scanner(connector, termStoreName(name), authorizations);
    } else {
      scannerBlob_ =
          Tables.batchScanner(connector, blobStoreName(name), authorizations, nbQueryThreads);
      scannerIndex_ =
          Tables.batchScanner(connector, termStoreName(name), authorizations, nbQueryThreads);
    }
  }

  @Override
  public void close() {

    if (scannerBlob_ != null) {
      scannerBlob_.close();
    }
    if (scannerIndex_ != null) {
      scannerIndex_.close();
    }

    scannerBlob_ = null;
    scannerIndex_ = null;
  }

  public void clear() {
    if (scannerBlob_ != null) {
      scannerBlob_.clearColumns();
      scannerBlob_.clearScanIterators();
    }
    if (scannerIndex_ != null) {
      scannerIndex_.clearColumns();
      scannerIndex_.clearScanIterators();
    }
  }

  public ScannerBase blob() {
    return scannerBlob_;
  }

  public ScannerBase index() {
    return scannerIndex_;
  }
}
