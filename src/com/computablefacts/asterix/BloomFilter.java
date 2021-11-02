package com.computablefacts.asterix;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * Implementation of a Bloom-filter, as described here: http://en.wikipedia.org/wiki/Bloom_filter
 *
 * For updates and bugfixes, see http://github.com/magnuss/java-bloomfilter
 *
 * Inspired by the SimpleBloomFilter-class written by Ian Clarke. This implementation provides a
 * more evenly distributed Hash-function by using a proper digest instead of the Java RNG. Many of
 * the changes were proposed in comments in his blog:
 * http://blog.locut.us/2008/01/12/a-decent-stand-alone-java-bloom-filter-implementation/
 *
 * @param <E> Object type that is to be inserted into the Bloom filter, e.g. String or Integer.
 * @author Magnus Skjegstad <magnus@skjegstad.com>
 */
@CheckReturnValue
final public class BloomFilter<E> implements Serializable {

  // encoding used for storing hash values
  private static final Charset CHARSET = StandardCharsets.UTF_8;

  // MD5 gives good enough accuracy in most circumstances
  // Change to SHA1 if it's needed
  private static final String HASH_NAME = "MD5";
  private static final MessageDigest DIGEST_FUNCTION;

  static { // The digest method is reused between instances
    @Var
    MessageDigest tmp;
    try {
      tmp = java.security.MessageDigest.getInstance(HASH_NAME);
    } catch (NoSuchAlgorithmException e) {
      tmp = null;
    }
    DIGEST_FUNCTION = tmp;
  }

  private final int bitSetSize_;
  private final double bitsPerElement_;
  private final int expectedNumberOfElements_; // expected (maximum) number of elements to be added
  private final int k_; // number of hash functions

  private int numberOfAddedElements_; // number of elements actually added to the Bloom filter
  private BitSet bitSet_;

  /**
   * Constructs a Bloom filter from an existing one.
   * 
   * @param falsePositiveProbability is the desired false positive probability.
   * @param expectedNumberOfElements is the expected number of elements in the Bloom filter.
   * @param actualNumberOfElements is the actual number of elements in the Bloom filter.
   * @param bitSet a previously initialized {@link BitSet}.
   */
  public BloomFilter(double falsePositiveProbability, int expectedNumberOfElements,
      int actualNumberOfElements, BitSet bitSet) {
    expectedNumberOfElements_ = expectedNumberOfElements;
    k_ = (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)));
    bitsPerElement_ = Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2);
    bitSetSize_ = (int) Math.ceil(bitsPerElement_ * expectedNumberOfElements_);
    numberOfAddedElements_ = actualNumberOfElements;
    bitSet_ = (BitSet) bitSet.clone(); // Here, new BitSet(bitSetSize_).size() == bitSet_.size()
  }

  /**
   * Constructs an empty Bloom filter. The total length of the Bloom filter will be c*n.
   *
   * @param c is the number of bits used per element.
   * @param n is the expected number of elements the filter will contain.
   * @param k is the number of hash functions used.
   */
  public BloomFilter(double c, int n, int k) {
    expectedNumberOfElements_ = n;
    k_ = k;
    bitsPerElement_ = c;
    bitSetSize_ = (int) Math.ceil(c * n);
    numberOfAddedElements_ = 0;
    bitSet_ = new BitSet(bitSetSize_);
  }

  /**
   * Constructs an empty Bloom filter. The optimal number of hash functions (k) is estimated from
   * the total size of the Bloom and the number of expected elements.
   *
   * @param bitSetSize defines how many bits should be used in total for the filter.
   * @param expectedNumberOElements defines the maximum number of elements the filter is expected to
   *        contain.
   */
  public BloomFilter(int bitSetSize, int expectedNumberOElements) {
    this(bitSetSize / (double) expectedNumberOElements, expectedNumberOElements,
        (int) Math.round((bitSetSize / (double) expectedNumberOElements) * Math.log(2.0)));
  }

  /**
   * Constructs an empty Bloom filter with a given false positive probability. The number of bits
   * per element and the number of hash functions is estimated to match the false positive
   * probability.
   *
   * @param falsePositiveProbability is the desired false positive probability.
   * @param expectedNumberOfElements is the expected number of elements in the Bloom filter.
   */
  public BloomFilter(double falsePositiveProbability, int expectedNumberOfElements) {
    // c = k / ln(2)
    // k = ceil(-log_2(false prob.))
    this(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2),
        expectedNumberOfElements,
        (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))));
  }

  /**
   * Construct a new Bloom filter based on existing Bloom filter data.
   *
   * @param bitSetSize defines how many bits should be used for the filter.
   * @param expectedNumberOfFilterElements defines the maximum number of elements the filter is
   *        expected to contain.
   * @param actualNumberOfFilterElements specifies how many elements have been inserted into the
   *        <code>filterData</code> BitSet.
   * @param filterData a BitSet representing an existing Bloom filter.
   */
  public BloomFilter(int bitSetSize, int expectedNumberOfFilterElements,
      int actualNumberOfFilterElements, BitSet filterData) {
    this(bitSetSize, expectedNumberOfFilterElements);
    bitSet_ = filterData;
    numberOfAddedElements_ = actualNumberOfFilterElements;
  }

  /**
   * Generates a digest based on the contents of a String.
   *
   * @param val specifies the input data.
   * @param charset specifies the encoding of the input data.
   * @return digest as long.
   */
  public static int createHash(String val, Charset charset) {
    return createHash(val.getBytes(charset));
  }

  /**
   * Generates a digest based on the contents of a String.
   *
   * @param val specifies the input data. The encoding is expected to be UTF-8.
   * @return digest as long.
   */
  public static int createHash(String val) {
    return createHash(val, CHARSET);
  }

  /**
   * Generates a digest based on the contents of an array of bytes.
   *
   * @param data specifies input data.
   * @return digest as long.
   */
  public static int createHash(byte[] data) {
    return createHashes(data, 1)[0];
  }

  /**
   * Generates digests based on the contents of an array of bytes and splits the result into 4-byte
   * int's and store them in an array. The digest function is called until the required number of
   * int's are produced. For each call to digest a salt is prepended to the data. The salt is
   * increased by 1 for each call.
   *
   * @param data specifies input data.
   * @param hashes number of hashes/int's to produce.
   * @return array of int-sized hashes
   */
  public static int[] createHashes(byte[] data, int hashes) {

    int[] result = new int[hashes];
    @Var
    int k = 0;
    @Var
    byte salt = 0;

    while (k < hashes) {

      @Var
      byte[] digest;

      synchronized (DIGEST_FUNCTION) {
        DIGEST_FUNCTION.update(salt);
        salt++;
        digest = DIGEST_FUNCTION.digest(data);
      }

      for (int i = 0; i < digest.length / 4 && k < hashes; i++) {

        @Var
        int h = 0;

        for (int j = (i * 4); j < (i * 4) + 4; j++) {
          h <<= 8;
          h |= ((int) digest[j]) & 0xFF;
        }

        result[k] = h;
        k++;
      }
    }
    return result;
  }

  /**
   * Compares the contents of two instances to see if they are equal.
   *
   * @param obj is the object to compare to.
   * @return True if the contents of the objects are equal.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BloomFilter<E> other = (BloomFilter<E>) obj;
    if (expectedNumberOfElements_ != other.expectedNumberOfElements_) {
      return false;
    }
    if (k_ != other.k_) {
      return false;
    }
    if (bitSetSize_ != other.bitSetSize_) {
      return false;
    }
    return bitSet_ == other.bitSet_ || (bitSet_ != null && bitSet_.equals(other.bitSet_));
  }

  /**
   * Calculates a hash code for this class.
   * 
   * @return hash code representing the contents of an instance of this class.
   */
  @Override
  public int hashCode() {
    @Var
    int hash = 7;
    hash = 61 * hash + (bitSet_ != null ? bitSet_.hashCode() : 0);
    hash = 61 * hash + expectedNumberOfElements_;
    hash = 61 * hash + bitSetSize_;
    hash = 61 * hash + k_;
    return hash;
  }

  /**
   * Calculates the expected probability of false positives based on the number of expected filter
   * elements and the size of the Bloom filter. <br />
   * <br />
   * The value returned by this method is the <i>expected</i> rate of false positives, assuming the
   * number of inserted elements equals the number of expected elements. If the number of elements
   * in the Bloom filter is less than the expected value, the true probability of false positives
   * will be lower.
   *
   * @return expected probability of false positives.
   */
  public double expectedFalsePositiveProbability() {
    return falsePositiveProbability(expectedNumberOfElements_);
  }

  /**
   * Calculate the probability of a false positive given the specified number of inserted elements.
   *
   * @param numberOfElements number of inserted elements.
   * @return probability of a false positive.
   */
  public double falsePositiveProbability(double numberOfElements) {
    // (1 - e^(-k * n / m)) ^ k
    return Math.pow((1 - Math.exp(-k_ * numberOfElements / (double) bitSetSize_)), k_);
  }

  /**
   * Get the current probability of a false positive. The probability is calculated from the size of
   * the Bloom filter and the current number of elements added to it.
   *
   * @return probability of false positives.
   */
  public double falsePositiveProbability() {
    return falsePositiveProbability(numberOfAddedElements_);
  }

  /**
   * Returns the value chosen for K.<br />
   * <br />
   * K is the optimal number of hash functions based on the size of the Bloom filter and the
   * expected number of inserted elements.
   *
   * @return optimal k.
   */
  public int k() {
    return k_;
  }

  /**
   * Sets all bits to false in the Bloom filter.
   */
  public void clear() {
    bitSet_.clear();
    numberOfAddedElements_ = 0;
  }

  /**
   * Adds an object to the Bloom filter. The output from the object's toString() method is used as
   * input to the hash functions.
   *
   * @param element is an element to register in the Bloom filter.
   */
  public void add(E element) {
    add(element.toString().getBytes(CHARSET));
  }

  /**
   * Adds an array of bytes to the Bloom filter.
   *
   * @param bytes array of bytes to add to the Bloom filter.
   */
  public void add(byte[] bytes) {
    int[] hashes = createHashes(bytes, k_);
    for (int hash : hashes) {
      bitSet_.set(Math.abs(hash % bitSetSize_), true);
    }
    numberOfAddedElements_++;
  }

  /**
   * Adds all elements from a Collection to the Bloom filter.
   * 
   * @param c Collection of elements.
   */
  public void addAll(Collection<? extends E> c) {
    for (E element : c) {
      add(element);
    }
  }

  /**
   * Returns true if the element could have been inserted into the Bloom filter. Use
   * getFalsePositiveProbability() to calculate the probability of this being correct.
   *
   * @param element element to check.
   * @return true if the element could have been inserted into the Bloom filter.
   */
  public boolean contains(E element) {
    return contains(element.toString().getBytes(CHARSET));
  }

  /**
   * Returns true if the array of bytes could have been inserted into the Bloom filter. Use
   * getFalsePositiveProbability() to calculate the probability of this being correct.
   *
   * @param bytes array of bytes to check.
   * @return true if the array could have been inserted into the Bloom filter.
   */
  public boolean contains(byte[] bytes) {
    int[] hashes = createHashes(bytes, k_);
    for (int hash : hashes) {
      if (!bitSet_.get(Math.abs(hash % bitSetSize_))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if all the elements of a Collection could have been inserted into the Bloom
   * filter. Use getFalsePositiveProbability() to calculate the probability of this being correct.
   * 
   * @param c elements to check.
   * @return true if all the elements in c could have been inserted into the Bloom filter.
   */
  public boolean containsAll(Collection<? extends E> c) {
    for (E element : c) {
      if (!contains(element)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Read a single bit from the Bloom filter.
   * 
   * @param bit the bit to read.
   * @return true if the bit is set, false if it is not.
   */
  public boolean getBit(int bit) {
    return bitSet_.get(bit);
  }

  /**
   * Set a single bit in the Bloom filter.
   * 
   * @param bit is the bit to set.
   * @param value If true, the bit is set. If false, the bit is cleared.
   */
  public void setBit(int bit, boolean value) {
    bitSet_.set(bit, value);
  }

  /**
   * Return the bit set used to store the Bloom filter.
   * 
   * @return bit set representing the Bloom filter.
   */
  public BitSet bitSet() {
    return bitSet_;
  }

  /**
   * Returns the number of bits in the Bloom filter. Use count() to retrieve the number of inserted
   * elements.
   *
   * @return the size of the bitset used by the Bloom filter.
   */
  public int size() {
    return bitSetSize_;
  }

  /**
   * Returns the number of elements added to the Bloom filter after it was constructed or after
   * clear() was called.
   *
   * @return number of elements added to the Bloom filter.
   */
  public int count() {
    return numberOfAddedElements_;
  }

  /**
   * Returns the actual number of elements inserted into the filter.
   *
   * @return actual number of elements.
   */
  public int actualNumberOfElements() {
    return numberOfAddedElements_;
  }

  /**
   * Returns the expected number of elements to be inserted into the filter. This value is the same
   * value as the one passed to the constructor.
   *
   * @return expected number of elements.
   */
  public int expectedNumberOfElements() {
    return expectedNumberOfElements_;
  }

  /**
   * Get expected number of bits per element when the Bloom filter is full. This value is set by the
   * constructor when the Bloom filter is created. See also getBitsPerElement().
   *
   * @return expected number of bits per element.
   */
  public double expectedBitsPerElement() {
    return bitsPerElement_;
  }

  /**
   * Get actual number of bits per element based on the number of elements that have currently been
   * inserted and the length of the Bloom filter. See also getExpectedBitsPerElement().
   *
   * @return number of bits per element.
   */
  public double bitsPerElement() {
    return bitSetSize_ / (double) numberOfAddedElements_;
  }
}
