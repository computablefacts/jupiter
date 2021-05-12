package com.computablefacts.jupiter.storage.datastore;

import static com.computablefacts.jupiter.storage.datastore.DataStore.blobStoreName;
import static com.computablefacts.jupiter.storage.datastore.DataStore.termStoreName;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.BatchScanner;
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
  private final List<ScannerBase> batchScannersBlob_ = new ArrayList<>();
  private final List<ScannerBase> batchScannersIndex_ = new ArrayList<>();
  private ScannerBase scannerBlob_;
  private ScannerBase scannerIndex_;

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

    batchScannersBlob_.forEach(ScannerBase::close);
    batchScannersIndex_.forEach(ScannerBase::close);

    batchScannersBlob_.clear();
    batchScannersIndex_.clear();
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

  /**
   * Be aware that "A BatchScanner instance will use no more threads than provided in the
   * construction of the BatchScanner implementation. Multiple invocations of iterator() will all
   * share the same resources of the instance. A new BatchScanner instance should be created to use
   * allocate additional threads."
   * 
   * @param nbQueryThreads the number of threads to use.
   * @return a {@link org.apache.accumulo.core.client.Scanner} if {@code nbQueryThreads} is less
   *         than or equals to 1. A {@link org.apache.accumulo.core.client.BatchScanner} otherwise.
   */
  public ScannerBase blob(int nbQueryThreads) {
    if (nbQueryThreads <= 1) {
      return blob();
    }

    BatchScanner scanner =
        Tables.batchScanner(connector_, blobStoreName(name_), authorizations_, nbQueryThreads);

    batchScannersBlob_.add(scanner);

    return scanner;
  }

  /**
   * Be aware that "A BatchScanner instance will use no more threads than provided in the
   * construction of the BatchScanner implementation. Multiple invocations of iterator() will all
   * share the same resources of the instance. A new BatchScanner instance should be created to use
   * allocate additional threads."
   *
   * @param nbQueryThreads the number of threads to use.
   * @return a {@link org.apache.accumulo.core.client.Scanner} if {@code nbQueryThreads} is less
   *         than or equals to 1. A {@link org.apache.accumulo.core.client.BatchScanner} otherwise.
   */
  public ScannerBase index(int nbQueryThreads) {
    if (nbQueryThreads <= 1) {
      return index();
    }

    BatchScanner scanner =
        Tables.batchScanner(connector_, termStoreName(name_), authorizations_, nbQueryThreads);

    batchScannersIndex_.add(scanner);

    return scanner;
  }
}
