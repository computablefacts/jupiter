package com.computablefacts.jupiter.storage.termstore;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.util.Pair;

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class ComparablePair<A extends Comparable<A>, B extends Comparable<B>>
    extends Pair<A, List<B>> implements Comparable<ComparablePair<A, B>> {

  public ComparablePair(Pair<A, List<B>> pair) {
    super(pair.getFirst(), pair.getSecond());
  }

  public ComparablePair(A a, List<B> bs) {
    super(a, bs);
  }

  public static <T extends Comparable<T>> int compare(Collection<T> l1, Collection<T> l2) {

    @Var
    int cmp = Integer.compare(l1.size(), l2.size());

    if (cmp != 0) {
      return cmp;
    }

    List<T> tmp1 = Lists.newArrayList(l1);
    List<T> tmp2 = Lists.newArrayList(l2);

    Collections.sort(tmp1);
    Collections.sort(tmp2);

    for (int i = 0; i < tmp1.size(); i++) {

      cmp = tmp1.get(i).compareTo(tmp2.get(i));

      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  @Override
  public int compareTo(@NotNull ComparablePair<A, B> pair) {

    int cmp = getFirst().compareTo(pair.getFirst());

    if (cmp != 0) {
      return cmp;
    }
    return compare(getSecond(), pair.getSecond());
  }
}
