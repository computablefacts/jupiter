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
