package com.computablefacts.asterix.trie;

import java.util.HashMap;
import java.util.Map;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class TrieNode<T> {

  final Map<T, TrieNode<T>> vocabulary_ = new HashMap<>();
  boolean isLeaf_ = false;

  public TrieNode() {}

  public boolean hasSiblings() {
    return !vocabulary_.isEmpty();
  }

  public boolean hasChildren(T t) {
    return vocabulary_.containsKey(t) && vocabulary_.get(t).hasSiblings();
  }

  public void clear() {
    vocabulary_.clear();
  }

  public void remove(T t) {
    vocabulary_.remove(t);
  }

  public boolean has(T t) {
    return vocabulary_.containsKey(t);
  }

  public TrieNode<T> get(T t) {
    return vocabulary_.get(t);
  }

  public void put(T t) {
    vocabulary_.put(t, new TrieNode<>());
  }
}
