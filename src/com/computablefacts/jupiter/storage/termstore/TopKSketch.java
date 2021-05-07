package com.computablefacts.jupiter.storage.termstore;

import java.util.List;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class TopKSketch {

  // See https://datasketches.apache.org/docs/Frequency/FrequentItemsErrorTable.html for details
  private static final int MAP_MAX_SIZE = 512;

  private final ItemsSketch<String> itemSketch_ = new ItemsSketch<>(MAP_MAX_SIZE);

  public TopKSketch() {}

  public static ItemsSketch<String> wrap(byte[] sketch) {
    return ItemsSketch.getInstance(Memory.wrap(sketch), new ArrayOfStringsSerDe());
  }

  public static ItemsSketch<String> union(List<byte[]> sketches) {
    ItemsSketch<String> union = new ItemsSketch<>(MAP_MAX_SIZE);
    for (byte[] sketch : sketches) {
      union.merge(wrap(sketch));
    }
    return union;
  }

  public void offer(String value, long count) {
    if (itemSketch_ != null && value != null) {
      itemSketch_.update(value, count);
    }
  }

  public byte[] toByteArray() {
    return itemSketch_ == null ? null : itemSketch_.toByteArray(new ArrayOfStringsSerDe());
  }
}
