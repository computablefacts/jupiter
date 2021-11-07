package com.computablefacts.asterix;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Var;

public class ViewTest {

  @Test
  public void testViewOfSet() {

    View<String> view = View.of(Sets.newHashSet("a", "b", "c"));

    Assert.assertEquals(Sets.newHashSet("a", "b", "c"), view.toSet());
  }

  @Test
  public void testViewOfMap() {

    Map<Integer, Set<String>> map = new HashMap<>();
    map.put(1, Sets.newHashSet("a"));
    map.put(2, Sets.newHashSet("ab"));
    map.put(3, Sets.newHashSet("abc", "abc"));
    map.put(4, Sets.newHashSet("abcd"));

    List<Map.Entry<Integer, Set<String>>> list = View.of(map).toList();

    Assert.assertTrue(list.contains(new AbstractMap.SimpleEntry<>(1, Sets.newHashSet("a"))));
    Assert.assertTrue(list.contains(new AbstractMap.SimpleEntry<>(2, Sets.newHashSet("ab"))));
    Assert
        .assertTrue(list.contains(new AbstractMap.SimpleEntry<>(3, Sets.newHashSet("abc", "abc"))));
    Assert.assertTrue(list.contains(new AbstractMap.SimpleEntry<>(4, Sets.newHashSet("abcd"))));
  }

  @Test
  public void testToList() {

    View<String> view = View.of(Lists.newArrayList("a", "b", "b", "c", "c", "c"));

    Assert.assertEquals(Lists.newArrayList("a", "b", "b", "c", "c", "c"), view.toList());
  }

  @Test
  public void testToSortedList() {

    View<String> view = View.of(Lists.newArrayList("a", "b", "b", "c", "c", "c"));

    Assert.assertEquals(Lists.newArrayList("c", "c", "c", "b", "b", "a"),
        view.toSortedList(Ordering.natural().reverse()));
  }

  @Test
  public void testToSet() {

    View<String> view = View.of(Lists.newArrayList("a", "b", "b", "c", "c", "c"));

    Assert.assertEquals(Sets.newHashSet("a", "b", "c"), view.toSet());
  }

  @Test
  public void testZipUnzipViewsHaveSameSize() {

    View<String> view1 = View.repeat("a").take(5);
    View<String> view2 = View.repeat("b").take(5);
    List<String> zipped = view1.zip(view2).map(e -> e.getKey() + e.getValue()).toList();

    Assert.assertEquals(Lists.newArrayList("ab", "ab", "ab", "ab", "ab"), zipped);

    Map.Entry<List<String>, List<String>> unzipped = View.of(zipped)
        .unzip(e -> new AbstractMap.SimpleEntry<>(e.substring(0, 1), e.substring(1, 2)));

    Assert.assertEquals(View.repeat("a").take(5).toList(), unzipped.getKey());
    Assert.assertEquals(View.repeat("b").take(5).toList(), unzipped.getValue());
  }

  @Test
  public void testZipUnzipLeftViewIsLonger() {

    View<String> view1 = View.repeat("a").take(10);
    View<String> view2 = View.repeat("b").take(5);
    List<String> zipped = view1.zip(view2).map(e -> e.getKey() + e.getValue()).toList();

    Assert.assertEquals(Lists.newArrayList("ab", "ab", "ab", "ab", "ab"), zipped);

    Map.Entry<List<String>, List<String>> unzipped = View.of(zipped)
        .unzip(e -> new AbstractMap.SimpleEntry<>(e.substring(0, 1), e.substring(1, 2)));

    Assert.assertEquals(View.repeat("a").take(5).toList(), unzipped.getKey());
    Assert.assertEquals(View.repeat("b").take(5).toList(), unzipped.getValue());
  }

  @Test
  public void testZipUnzipRightViewIsLonger() {

    View<String> view1 = View.repeat("a").take(5);
    View<String> view2 = View.repeat("b").take(10);
    List<String> zipped = view1.zip(view2).map(e -> e.getKey() + e.getValue()).toList();

    Assert.assertEquals(Lists.newArrayList("ab", "ab", "ab", "ab", "ab"), zipped);

    Map.Entry<List<String>, List<String>> unzipped = View.of(zipped)
        .unzip(e -> new AbstractMap.SimpleEntry<>(e.substring(0, 1), e.substring(1, 2)));

    Assert.assertEquals(View.repeat("a").take(5).toList(), unzipped.getKey());
    Assert.assertEquals(View.repeat("b").take(5).toList(), unzipped.getValue());
  }

  @Test
  public void testRepeat() {

    Set<String> set = View.repeat("a").take(5).toSet();
    List<String> list = View.repeat("a").take(5).toList();

    Assert.assertEquals(Sets.newHashSet("a"), set);
    Assert.assertEquals(Lists.newArrayList("a", "a", "a", "a", "a"), list);
  }

  @Test
  public void testRange() {

    List<Integer> list = View.range(100, 105).toList();

    Assert.assertEquals(Lists.newArrayList(100, 101, 102, 103, 104), list);
  }

  @Test
  public void testIterate() {

    Set<Integer> set = View.iterate(1, x -> x + 1).take(5).toSet();
    List<Integer> list = View.iterate(1, x -> x + 1).take(5).toList();

    Assert.assertEquals(Sets.newHashSet(1, 2, 3, 4, 5), set);
    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4, 5), list);
  }

  @Test
  public void testIndex() {

    Set<Map.Entry<Integer, String>> set = View.repeat("a").take(3).index().toSet();
    List<Map.Entry<Integer, String>> list = View.repeat("a").take(3).index().toList();

    Assert.assertEquals(Sets.newHashSet(new AbstractMap.SimpleEntry<>(1, "a"),
        new AbstractMap.SimpleEntry<>(2, "a"), new AbstractMap.SimpleEntry<>(3, "a")), set);
    Assert.assertEquals(Lists.newArrayList(new AbstractMap.SimpleEntry<>(1, "a"),
        new AbstractMap.SimpleEntry<>(2, "a"), new AbstractMap.SimpleEntry<>(3, "a")), list);
  }

  @Test
  public void testContains() {

    View<Integer> view = View.range(100, 105);

    Assert.assertTrue(view.contains(x -> x == 103));
  }

  @Test
  public void testAnyMatch() {

    View<Integer> view = View.range(100, 105);

    Assert.assertTrue(view.anyMatch(x -> x % 2 == 0));
  }

  @Test
  public void testAllMatch() {

    View<Integer> view = View.range(100, 105).filter(x -> x % 2 == 0);

    Assert.assertTrue(view.allMatch(x -> x % 2 == 0));
  }

  @Test
  public void testFindAll() {

    List<Integer> list = View.iterate(1, x -> x + 1).take(5).findAll(x -> x % 2 == 0).toList();

    Assert.assertEquals(Lists.newArrayList(2, 4), list);
  }

  @Test
  public void testFindFirst() {

    Optional<Integer> opt = View.iterate(1, x -> x + 1).take(5).findFirst(x -> x % 2 == 0);

    Assert.assertEquals(2, (long) opt.get());
  }

  @Test
  public void testReduce() {

    int value = View.iterate(1, x -> x + 1).take(100).reduce(0, (c, x) -> c + x);

    Assert.assertEquals(5050, value);
  }

  @Test
  public void testPrepend() {

    List<Integer> list = View.iterate(2, x -> x + 1).take(3).prepend(1).toList();

    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4), list);
  }

  @Test
  public void testAppend() {

    List<Integer> list = View.iterate(1, x -> x + 1).take(3).append(4).toList();

    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4), list);
  }

  @Test
  public void testGroupDistinctByLength() {

    View<String> view = View.of(Lists.newArrayList("a", "a", "ab", "abc", "abc", "abcd"));
    Map<Integer, Set<String>> actual = view.groupDistinct(x -> x.length());
    Map<Integer, Set<String>> expected = new HashMap<>();
    expected.put(1, Sets.newHashSet("a"));
    expected.put(2, Sets.newHashSet("ab"));
    expected.put(3, Sets.newHashSet("abc", "abc"));
    expected.put(4, Sets.newHashSet("abcd"));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGroupAllByLength() {

    View<String> view = View.of(Lists.newArrayList("a", "a", "ab", "abc", "abc", "abcd"));
    Map<Integer, List<String>> actual = view.groupAll(x -> x.length());
    Map<Integer, List<String>> expected = new HashMap<>();
    expected.put(1, Lists.newArrayList("a", "a"));
    expected.put(2, Lists.newArrayList("ab"));
    expected.put(3, Lists.newArrayList("abc", "abc"));
    expected.put(4, Lists.newArrayList("abcd"));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTakeAll() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.take(5).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"), list);
  }

  @Test
  public void testTakeNone() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.take(0).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testTakeNOnEmptyView() {

    View<String> view = View.of(Collections.emptyList());
    List<String> list = view.take(5).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testTakeN() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.take(2).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab"), list);
  }

  @Test
  public void testTakeWhileOnEmptyView() {

    View<String> view = View.of(Collections.emptyList());
    List<String> list = view.takeWhile(w -> w.length() < 6).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testTakeWhileExhaustsView() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.takeWhile(w -> w.length() < 6).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"), list);
  }

  @Test
  public void testTakeWhile() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.takeWhile(w -> w.length() == 1).toList();

    Assert.assertEquals(Lists.newArrayList("a"), list);
  }

  @Test
  public void testTakeUntil() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.takeUntil(w -> w.length() > 3).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab", "abc"), list);
  }

  @Test
  public void testDropAll() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.drop(5).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testDropNone() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.drop(0).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"), list);
  }

  @Test
  public void testDropNOnEmptyView() {

    View<String> view = View.of(Collections.emptyList());
    List<String> list = view.drop(2).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testDropN() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.drop(2).toList();

    Assert.assertEquals(Lists.newArrayList("abc", "abcd", "abcde"), list);
  }

  @Test
  public void testDropWhileOnEmptyView() {

    View<String> view = View.of(Collections.emptyList());
    List<String> list = view.dropWhile(w -> w.length() < 6).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testDropWhileExhaustsView() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.dropWhile(w -> w.length() < 6).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testDropWhile() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.dropWhile(w -> w.length() < 3).toList();

    Assert.assertEquals(Lists.newArrayList("abc", "abcd", "abcde"), list);
  }

  @Test
  public void testDropUntil() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.dropUntil(w -> w.length() >= 3).toList();

    Assert.assertEquals(Lists.newArrayList("abc", "abcd", "abcde"), list);
  }

  @Test
  public void testDropAndTake() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.drop(2).take(1).toList();

    Assert.assertEquals(Lists.newArrayList("abc"), list);
  }

  @Test
  public void testFilterView() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = view.filter(w -> w.length() % 2 == 0).toList();

    Assert.assertEquals(Lists.newArrayList("ab", "abcd"), list);
  }

  @Test
  public void testMapView() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<Integer> list = view.map(w -> w.length()).toList();

    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4, 5), list);
  }

  @Test
  public void testConcatViews() {

    View<String> left1 = View.of(Lists.newArrayList("a", "ab", "abc"));
    View<String> right1 = View.of(Lists.newArrayList("abcd", "abcde"));
    List<String> list1 = left1.concat(right1).toList();

    Assert.assertEquals(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"), list1);

    View<String> left2 = View.of(Lists.newArrayList("a", "ab", "abc"));
    View<String> right2 = View.of(Lists.newArrayList("abcd", "abcde"));
    List<String> list2 = right2.concat(left2).toList();

    Assert.assertEquals(Lists.newArrayList("abcd", "abcde", "a", "ab", "abc"), list2);
  }

  @Test
  public void testPartitionView() {

    View<String> view = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<List<String>> list = view.partition(3).toList();

    Assert.assertEquals(Lists.newArrayList(Lists.newArrayList("a", "ab", "abc"),
        Lists.newArrayList("abcd", "abcde")), list);
  }

  @Test
  public void testDiffEmptyViewAgainstEmptyView() {

    View<String> left = View.of(Lists.newArrayList());
    View<String> right = View.of(Lists.newArrayList());
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertTrue(diff.isEmpty());
  }

  @Test
  public void testDiffEmptyViewAgainstNonEmptyView() {

    View<String> left = View.of(Lists.newArrayList());
    View<String> right = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertTrue(diff.isEmpty());
  }

  @Test
  public void testDiffNonEmptyViewAgainstEmptyView() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList());
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "b", "c", "d", "e"), diff);
  }

  @Test
  public void testDiffViewAgainstItself() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertTrue(diff.isEmpty());
  }

  @Test
  public void testDiffRemovesElementsInEvenPositions() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "c", "e"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("b", "d"), diff);
  }

  @Test
  public void testDiffRemovesElementsInOddPositions() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("b", "d"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), diff);
  }

  @Test
  public void testDiffLeftViewHasMoreElements() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e", "f"));
    View<String> right = View.of(Lists.newArrayList("a", "c", "e"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("b", "d", "f"), diff);
  }

  @Test
  public void testDiffRightViewHasMoreElements() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("b", "d", "f", "h"));
    List<String> diff = left.diffSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), diff);
  }

  @Test
  public void testDedupEmptyView() {

    List<String> list = View.<String>of(Collections.emptyList()).dedupSorted().toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testDedupViewWithoutDuplicates() {

    List<String> actual =
        View.of(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g", "h")).dedupSorted().toList();
    List<String> expected = Lists.newArrayList("a", "b", "c", "d", "e", "f", "g", "h");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDedupViewWithDuplicates() {

    List<String> actual = View.of(
        Lists.newArrayList("a", "a", "b", "b", "b", "c", "c", "c", "c", "d", "e", "f", "g", "h"))
        .dedupSorted().toList();
    List<String> expected = Lists.newArrayList("a", "b", "c", "d", "e", "f", "g", "h");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFlattenEmptyView() {

    List<String> actual = View.<View<String>>of(Collections.emptyList()).flatten(l -> l).toList();

    Assert.assertTrue(actual.isEmpty());
  }

  @Test
  public void testFlattenFlatView() {

    List<String> actual = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"))
        .flatten(l -> View.of(Lists.newArrayList(l))).toList();
    List<String> expected = Lists.newArrayList("a", "ab", "abc", "abcd", "abcde");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFlattenViewOfViews() {

    List<String> actual = View.of(Lists.newArrayList(View.of(Lists.newArrayList("a", "ab")),
        View.of(Lists.newArrayList("abc", "abcd", "abcde")))).flatten(l -> l).toList();
    List<String> expected = Lists.newArrayList("a", "ab", "abc", "abcd", "abcde");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testIntersectEmptyViewWithEmptyView() {

    View<String> left = View.of(Collections.emptyList());
    View<String> right = View.of(Collections.emptyList());
    List<String> list = left.intersectSorted(right).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testIntersectEmptyViewWithNonEmptyView() {

    View<String> left = View.of(Collections.emptyList());
    View<String> right = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    List<String> list = left.intersectSorted(right).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testIntersectNonEmptyViewWithEmptyView() {

    View<String> left = View.of(Lists.newArrayList("a", "ab", "abc", "abcd", "abcde"));
    View<String> right = View.of(Collections.emptyList());
    List<String> list = left.intersectSorted(right).toList();

    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testIntersectKeepsOnlyElementsInEvenPositions() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("b", "d"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("b", "d"), intersection);
  }

  @Test
  public void testIntersectKeepsOnlyElementsInOddPositions() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "c", "e"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), intersection);
  }

  @Test
  public void testIntersectLeftViewDoesNotOverlapRightView() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e", "f"));
    View<String> right = View.of(Lists.newArrayList("0", "1", "2"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertTrue(intersection.isEmpty());
  }

  @Test
  public void testIntersectLeftViewOverlapsRightView() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e", "f"));
    View<String> right = View.of(Lists.newArrayList("0", "1", "2", "a", "c", "e"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), intersection);
  }

  @Test
  public void testIntersectLeftViewHasDuplicatedElements() {

    View<String> left = View.of(Lists.newArrayList("a", "a", "b", "b", "b", "c", "c", "c", "c", "d",
        "d", "d", "d", "d", "e", "e", "e", "e", "e", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "c", "e"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), intersection);
  }

  @Test
  public void testIntersectRightViewHasDuplicatedElements() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "a", "c", "c", "c", "e", "e", "e", "e"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), intersection);
  }

  @Test
  public void testIntersectBothViewsHaveDuplicatedElements() {

    View<String> left = View.of(Lists.newArrayList("a", "b", "c", "c", "c", "d", "e"));
    View<String> right = View.of(Lists.newArrayList("a", "a", "a", "c", "e"));
    List<String> intersection = left.intersectSorted(right).toList();

    Assert.assertEquals(Lists.newArrayList("a", "c", "e"), intersection);
  }

  @Test
  public void testBufferizeWithCapacity() {

    View<String> view = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    List<String> list = view.bufferize(3).toList();

    Assert.assertEquals(Lists.newArrayList("a", "b", "c", "d", "e"), list);
  }

  @Test
  public void testBufferizeWithoutCapacity() {

    View<String> view = View.of(Lists.newArrayList("a", "b", "c", "d", "e"));
    List<String> list = view.bufferize(0).toList();

    Assert.assertEquals(Lists.newArrayList("a", "b", "c", "d", "e"), list);
  }

  @Test
  public void testBufferizeViewWithNullElement() {

    View<String> view = View.of(Lists.newArrayList("a", "b", null, "d", "e"));
    List<String> list = view.bufferize(3).toList();

    Assert.assertEquals(Lists.newArrayList("a", "b", null, "d", "e"), list);
  }

  @Test
  public void testBufferizeView() {

    View<Integer> view = View.iterate(1, x -> x + 1);
    List<Integer> list = view.bufferize(3).map(x -> {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // FALL THROUGH
      }
      return x;
    }).take(10).toList();

    Assert.assertEquals(10, list.size());
  }

  @Test
  public void testForEachRemainingWithoutBreaker() {

    List<String> result = new ArrayList<>();
    View<String> view = View.of(Stream.of("cat", "dog", "elephant", "fox", "rabbit", "duck"));

    view.forEachRemaining((elem) -> {
      if (elem.length() % 2 == 0) {
        result.add(elem);
      }
    });

    Assert.assertEquals(Lists.newArrayList("elephant", "rabbit", "duck"), result);
  }

  @Test
  public void testForEachRemainingWithBreaker() {

    List<String> result = new ArrayList<>();
    View<String> view = View.of(Stream.of("cat", "dog", "elephant", "fox", "rabbit", "duck"));

    view.forEachRemaining((elem, breaker) -> {
      if (elem.length() % 2 == 0) {
        breaker.stop();
      } else {
        result.add(elem);
      }
    });

    Assert.assertEquals(Lists.newArrayList("cat", "dog"), result);
  }

  @Test
  public void testMerge() {

    View<String> view1 = View.of(Lists.newArrayList("0", "2", "4"));
    View<String> view2 = View.of(Lists.newArrayList("1", "3", "5"));
    View<String> view3 = View.of(Lists.newArrayList("6", "7", "8", "9", "b", "d", "f"));
    View<String> view4 = View.of(Lists.newArrayList("a", "c", "e", "g"));

    View<String> merged =
        view1.mergeSorted(Lists.newArrayList(view2, view3, view4), String::compareTo);

    Assert.assertEquals(Lists.newArrayList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a",
        "b", "c", "d", "e", "f", "g"), merged.toList());
  }

  @Test
  public void testOverlappingWindow() {

    View<String> view = View.of(Lists.newArrayList("1", "2", "3", "4", "5", "6", "7"));
    List<ImmutableList<String>> windows = view.overlappingWindow(3).toList();

    Assert.assertEquals(7, windows.size());
    Assert.assertTrue(windows.contains(ImmutableList.of("1", "2", "3")));
    Assert.assertTrue(windows.contains(ImmutableList.of("2", "3", "4")));
    Assert.assertTrue(windows.contains(ImmutableList.of("3", "4", "5")));
    Assert.assertTrue(windows.contains(ImmutableList.of("4", "5", "6")));
    Assert.assertTrue(windows.contains(ImmutableList.of("5", "6", "7")));
    Assert.assertTrue(windows.contains(ImmutableList.of("6", "7")));
    Assert.assertTrue(windows.contains(ImmutableList.of("7")));
  }

  @Test
  public void testNonOverlappingWindow() {

    View<String> view = View.of(Lists.newArrayList("1", "2", "3", "4", "5", "6", "7"));
    List<ImmutableList<String>> windows = view.nonOverlappingWindow(3).toList();

    Assert.assertEquals(3, windows.size());
    Assert.assertTrue(windows.contains(ImmutableList.of("1", "2", "3")));
    Assert.assertTrue(windows.contains(ImmutableList.of("4", "5", "6")));
    Assert.assertTrue(windows.contains(ImmutableList.of("7")));
  }

  @Test
  public void testGroupSorted() {

    List<Integer> list = new ArrayList<>();
    list.add(1);

    @Var
    List<List<Integer>> groups =
        View.of(list).groupSorted(Integer::equals).map(View::toList).toList();

    Assert.assertEquals(1, groups.size());
    Assert.assertTrue(groups.contains(ImmutableList.of(1)));

    list.add(2);
    list.add(2);

    groups = View.of(list).groupSorted(Integer::equals).map(View::toList).toList();

    Assert.assertEquals(2, groups.size());
    Assert.assertEquals(ImmutableList.of(1), groups.get(0));
    Assert.assertEquals(ImmutableList.of(2, 2), groups.get(1));

    list.add(3);
    list.add(3);
    list.add(3);

    groups = View.of(list).groupSorted(Integer::equals).map(View::toList).toList();

    Assert.assertEquals(3, groups.size());
    Assert.assertEquals(ImmutableList.of(1), groups.get(0));
    Assert.assertEquals(ImmutableList.of(2, 2), groups.get(1));
    Assert.assertEquals(ImmutableList.of(3, 3, 3), groups.get(2));
  }

  @Test
  public void testPeek() {

    List<String> valuesPeeked = new ArrayList<>();
    View<String> view =
        View.of(Lists.newArrayList("1", "2", "3", "4", "5", "6", "7")).peek(valuesPeeked::add);

    Assert.assertTrue(valuesPeeked.isEmpty());

    List<String> valuesMaterialized = view.toList();

    Assert.assertEquals(valuesPeeked, valuesMaterialized);
  }
}
