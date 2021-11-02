package com.computablefacts.asterix.trie;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class Trie<T> {

  private final TrieNode<T> root_ = new TrieNode<>();

  public Trie() {}

  public void insert(List<T> elements) {
    insert(root_, elements);
  }

  @CanIgnoreReturnValue
  private boolean insert(TrieNode<T> head, List<T> elements) {

    Preconditions.checkNotNull(head, "head should not be null");
    Preconditions.checkNotNull(elements, "elements should not be null");

    if (elements.isEmpty()) {
      return false;
    }

    @Var
    TrieNode<T> node = head;

    for (T t : elements) {
      if (!node.has(t)) {
        node.put(t);
      }
      node = node.get(t);
    }

    node.isLeaf_ = true;
    return true;
  }

  public boolean contains(List<T> elements) {
    return contains(root_, elements);
  }

  private boolean contains(TrieNode<T> head, List<T> elements) {

    Preconditions.checkNotNull(elements, "elements should not be null");

    if (head == null) {
      return false;
    }

    @Var
    TrieNode<T> node = head;

    for (T t : elements) {
      node = node.get(t);
      if (node == null) {
        return false;
      }
    }
    return node.isLeaf_;
  }

  public void delete(List<T> elements) {
    delete(root_, elements);
  }

  @CanIgnoreReturnValue
  private boolean delete(TrieNode<T> head, List<T> elements) {

    Preconditions.checkNotNull(elements, "elements should not be null");

    if (head == null) {
      return false;
    }

    if (!elements.isEmpty()) {
      if (head.has(elements.get(0))
          && delete(head.get(elements.get(0)), elements.subList(1, elements.size()))
          && !head.isLeaf_) {
        if (!head.hasChildren(elements.get(0))) {
          head.remove(elements.get(0));
          return true;
        }
        return false;
      }
    }

    if (elements.isEmpty() && head.isLeaf_) {
      if (!head.hasSiblings()) {
        head.clear();
        return true;
      }
      head.isLeaf_ = false;
    }
    return false;
  }

  public void removeIf(Predicate<T> fn) {
    for (List<T> path : paths()) {
      if (path.stream().anyMatch(fn)) {
        delete(path);
      }
    }
  }

  public List<List<T>> paths() {
    return find(root_, new ArrayList<>());
  }

  private List<List<T>> find(TrieNode<T> head, List<T> path) {

    Preconditions.checkNotNull(path, "path should not be null");

    if (head == null) {
      return new ArrayList<>();
    }

    @Var
    TrieNode<T> node = head;
    List<List<T>> allPaths = new ArrayList<>();

    if (node.isLeaf_) {
      allPaths.add(path);
    }

    for (T t : node.vocabulary_.keySet()) {

      List<T> newPath = new ArrayList<>(path);
      newPath.add(t);

      List<List<T>> newPaths = find(node.get(t), newPath);

      allPaths.addAll(newPaths);
    }
    return allPaths;
  }
}
