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

@CheckReturnValue
public class Scanners implements AutoCloseable {

  private final Configurations configurations_;
  private final Connector connector_;
  private final String name_;
  private final Authorizations authorizations_;
  private ScannerBase scannerBlob_;
  private ScannerBase scannerIndex_;
  private ScannerBase batchScannerBlob_;
  private ScannerBase batchScannerIndex_;

  public Scanners(Configurations configurations, String name, Authorizations authorizations) {

    Preconditions.checkNotNull(configurations, "configurations should neither be null nor empty");
    Preconditions.checkNotNull(name, "name should neither be null nor empty");

    configurations_ = configurations;
    connector_ = configurations_.connector();
    authorizations_ = authorizations == null ? Authorizations.EMPTY : authorizations;
    name_ = name;
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

    if (batchScannerBlob_ != null) {
      batchScannerBlob_.close();
    }
    if (batchScannerIndex_ != null) {
      batchScannerIndex_.close();
    }

    batchScannerBlob_ = null;
    batchScannerIndex_ = null;
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
    if (batchScannerBlob_ != null) {
      batchScannerBlob_.clearColumns();
      batchScannerBlob_.clearScanIterators();
    }
    if (batchScannerIndex_ != null) {
      batchScannerIndex_.clearColumns();
      batchScannerIndex_.clearScanIterators();
    }
  }

  public ScannerBase blob() {
    if (scannerBlob_ == null) {
      scannerBlob_ = Tables.scanner(connector_, blobStoreName(name_), authorizations_);
    }
    return scannerBlob_;
  }

  public ScannerBase index() {
    if (scannerIndex_ == null) {
      scannerIndex_ = Tables.scanner(connector_, termStoreName(name_), authorizations_);
    }
    return scannerIndex_;
  }

  public ScannerBase blob(int nbQueryThreads) {
    if (nbQueryThreads <= 1) {
      return blob();
    }
    if (batchScannerBlob_ == null) {
      batchScannerBlob_ =
          Tables.batchScanner(connector_, blobStoreName(name_), authorizations_, nbQueryThreads);
    }
    return batchScannerBlob_;
  }

  public ScannerBase index(int nbQueryThreads) {
    if (nbQueryThreads <= 1) {
      return index();
    }
    if (batchScannerIndex_ == null) {
      batchScannerIndex_ =
          Tables.batchScanner(connector_, termStoreName(name_), authorizations_, nbQueryThreads);
    }
    return batchScannerIndex_;
  }
}
