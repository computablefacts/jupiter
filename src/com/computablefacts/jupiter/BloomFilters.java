package com.computablefacts.jupiter;

import java.util.BitSet;
import java.util.Stack;

import org.apache.commons.io.Charsets;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

@CheckReturnValue
final public class BloomFilters<T> {

  private final static double FALSE_POSITIVE_PROBABILITY_MIN = 0.1;
  private final static double FALSE_POSITIVE_PROBABILITY_MAX = 0.3;
  private final static int EXPECTED_NUMBER_OF_ELEMENTS = 1000000;
  private final static int BITSET_SIZE = new BitSet(
      (int) Math.ceil((int) Math.ceil(-(Math.log(FALSE_POSITIVE_PROBABILITY_MIN) / Math.log(2)))
          / Math.log(2) * EXPECTED_NUMBER_OF_ELEMENTS)).size();
  private final static int BYTE_ARRAY_SIZE = BITSET_SIZE / 8;
  private final Stack<BloomFilter<T>> filters_ = new Stack<>();

  public BloomFilters() {}

  public BloomFilters(BloomFilters<T> bloomFilters) {
    if (bloomFilters != null) {
      for (BloomFilter<T> bf : bloomFilters.filters_) {
        filters_.push(new BloomFilter<>(bf.falsePositiveProbability(),
            bf.expectedNumberOfElements(), bf.actualNumberOfElements(), bf.bitSet()));
      }
    }
  }

  public static <T> String toString(BloomFilters<T> bloomFilters) {

    StringBuilder builder = new StringBuilder().append(bloomFilters.filters_.size()).append('¤');

    for (BloomFilter<T> filter : bloomFilters.filters_) {
      builder.append(filter.actualNumberOfElements());
      builder.append('¤');
      builder.append(serializeBitSet(filter.bitSet()));
    }
    return builder.toString();
  }

  public static <T> BloomFilters<T> fromString(@Var String bloomFilters) {

    if (bloomFilters == null) {
      return null;
    }

    @Var
    int separator = bloomFilters.indexOf('¤');
    if (separator < 0) {
      return null;
    }

    int nbFilters = Integer.parseInt(bloomFilters.substring(0, separator), 10);
    if (nbFilters <= 0) {
      return null;
    }

    bloomFilters = bloomFilters.substring(separator + 1);
    BloomFilters<T> bfs = new BloomFilters<>();

    while (bfs.filters_.size() < nbFilters) {

      separator = bloomFilters.indexOf('¤');

      int actualNumberOfElements = Integer.parseInt(bloomFilters.substring(0, separator), 10);
      BitSet bitSet =
          deserializeBitSet(bloomFilters.substring(separator + 1, separator + 1 + BYTE_ARRAY_SIZE));

      bfs.filters_.push(new BloomFilter<>(FALSE_POSITIVE_PROBABILITY_MIN,
          EXPECTED_NUMBER_OF_ELEMENTS, actualNumberOfElements, bitSet));

      bloomFilters = bloomFilters.substring(separator + 1 + BYTE_ARRAY_SIZE);
    }
    return bfs;
  }

  private static String serializeBitSet(BitSet bitSet) {

    Preconditions.checkNotNull(bitSet, "bitSet should not be null");

    byte[] bytes = new byte[BYTE_ARRAY_SIZE];

    for (int i = 0; i < bytes.length; ++i) {
      for (int j = 0; j < 8; ++j) {
        if (bitSet.get(i * 8 + j)) {
          bytes[i] |= 1 << j;
        }
      }
    }

    // ISO-8859-1 has a 1-to-1 mapping between bytes and characters
    return new String(bytes, Charsets.ISO_8859_1);
  }

  private static BitSet deserializeBitSet(String str) {

    Preconditions.checkNotNull(str, "str should not be null");

    // ISO-8859-1 has a 1-to-1 mapping between bytes and characters
    byte[] bytes = str.getBytes(Charsets.ISO_8859_1);
    BitSet bitSet = new BitSet(BITSET_SIZE);

    for (int i = 0; i < bytes.length; ++i) {
      if (bytes[i] != 0) {
        for (int j = 0; j < 8; ++j) {
          if ((bytes[i] & (1 << j)) != 0) {
            bitSet.set(i * 8 + j);
          }
        }
      }
    }
    return bitSet;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BloomFilters)) {
      return false;
    }
    BloomFilters bfs = (BloomFilters) obj;
    return Objects.equal(filters_, bfs.filters_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(filters_);
  }

  /**
   * Puts an element into this {@link BloomFilters<T>}. Ensures that subsequent invocations of
   * {@link #mightContain(Object)} with the same element will always return {@code true}.
   *
   * This method does not attempt to check if the element has previously been stored in one of the
   * underlying Bloom filters before trying to insert it again.
   * 
   * @param object the element to store.
   */
  public void put(T object) {
    if (object != null) {
      if (filters_.isEmpty()
          || filters_.peek().falsePositiveProbability() >= FALSE_POSITIVE_PROBABILITY_MAX) {
        filters_.push(newBloomFilter());
      }
      filters_.peek().add(object);
    }
  }

  /**
   * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter, {@code
   * false} if this is <i>definitely</i> not the case.
   *
   * @param obj the element to search for.
   * @return Returns {@code true} if the element <i>might</i> have been put in this Bloom filter,
   *         {@code false} if this is <i>definitely</i> not the case.
   */
  public boolean mightContain(T obj) {
    return obj != null && filters_.stream().anyMatch(filter -> filter.contains(obj));
  }

  /**
   * The rough space occupied by each bloom filter is about 600 kb.
   * 
   * @return {@link BloomFilter<T>}
   */
  private BloomFilter<T> newBloomFilter() {
    return new BloomFilter<>(FALSE_POSITIVE_PROBABILITY_MIN, EXPECTED_NUMBER_OF_ELEMENTS);
  }
}
