package com.computablefacts.jupiter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class StreamsTest {

  @Test
  public void testForEach() {

    Stream<String> initialStream = Stream.of("cat", "dog", "elephant", "fox", "rabbit", "duck");
    List<String> result = new ArrayList<>();

    Streams.forEach(initialStream, (elem, breaker) -> {
      if (elem.length() % 2 == 0) {
        breaker.stop();
      } else {
        result.add(elem);
      }
    });

    Assert.assertEquals(Lists.newArrayList("cat", "dog"), result);
  }
}
