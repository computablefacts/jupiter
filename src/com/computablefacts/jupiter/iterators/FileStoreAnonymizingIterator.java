package com.computablefacts.jupiter.iterators;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class FileStoreAnonymizingIterator extends BlobStoreAnonymizingIterator {

  public FileStoreAnonymizingIterator() {}

  @Override
  protected AnonymizingIterator create() {
    return new FileStoreAnonymizingIterator();
  }
}
