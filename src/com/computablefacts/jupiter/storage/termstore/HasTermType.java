package com.computablefacts.jupiter.storage.termstore;

public interface HasTermType {

  int termType();

  boolean isUnknown();

  boolean isString();

  boolean isNumber();

  boolean isDate();
}
