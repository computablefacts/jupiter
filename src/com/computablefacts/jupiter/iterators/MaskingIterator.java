package com.computablefacts.jupiter.iterators;

import static com.computablefacts.jupiter.storage.Constants.MURMUR3_128;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;

import com.computablefacts.jupiter.Users;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public abstract class MaskingIterator
    implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

  private static final String AUTHS_CRITERION = "a";
  private static final String SALT_CRITERION = "s";

  private SortedKeyValueIterator<Key, Value> source_;
  private Map<String, String> options_;
  private Key topKey_;
  private Value topValue_;
  private Set<String> auths_;

  public MaskingIterator() {}

  public static void setAuthorizations(IteratorSetting setting, Authorizations authorizations) {
    if (authorizations != null) {
      setting.addOption(AUTHS_CRITERION, authorizations.toString());
    }
  }

  public static void setSalt(IteratorSetting setting, String salt) {
    if (salt != null) {
      setting.addOption(SALT_CRITERION, salt);
    }
  }

  /**
   * Hash a given {@link String}.
   *
   * @param salt salt.
   * @param value {@link String}.
   * @return hashed value.
   */
  public static String hash(String salt, String value) {

    Hasher hasher = MURMUR3_128.newHasher();

    if (salt != null) {
      hasher.putString(salt, StandardCharsets.UTF_8);
    }
    if (value != null) {
      hasher.putString(value, StandardCharsets.UTF_8);
    }
    return hasher.hash().toString();
  }

  /**
   * Hash a given {@link Value}.
   *
   * @param salt salt.
   * @param value {@link Value}.
   * @return hashed value.
   */
  private static String hash(String salt, Value value) {

    Hasher hasher = MURMUR3_128.newHasher();

    if (salt != null) {
      hasher.putString(salt, StandardCharsets.UTF_8);
    }
    if (value != null) {
      hasher.putBytes(value.get());
    }
    return hasher.hash().toString();
  }

  /**
   * Mask a given {@link String}.
   *
   * @param salt salt.
   * @param value {@link String}.
   * @return hashed value.
   */
  protected static String mask(String salt, String value) {
    return "MASKED_" + hash(salt, value);
  }

  /**
   * Mask a given {@link Value}.
   *
   * @param salt salt.
   * @param value {@link Value}.
   * @return hashed value.
   */
  protected static String mask(String salt, Value value) {
    return "MASKED_" + hash(salt, value);
  }

  @Override
  public IteratorOptions describeOptions() {
    return null; // TODO
  }

  @Override
  public boolean validateOptions(Map<String, String> map) {
    return map.containsKey(AUTHS_CRITERION);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment environment) {
    source_ = source;
    options_ = new HashMap<>(options);
  }

  @Override
  public boolean hasTop() {
    return topKey_ != null;
  }

  @Override
  public void next() throws IOException {
    if (source_.hasTop()) {
      setTopKeyValue(source_.getTopKey(), source_.getTopValue());
      source_.next();
    } else {
      topKey_ = null;
      topValue_ = null;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source_.seek(range, columnFamilies, inclusive);
    next();
  }

  @Override
  public Key getTopKey() {
    return topKey_;
  }

  protected void setTopKey(Key key) {
    topKey_ = new Key(key);
  }

  @Override
  public Value getTopValue() {
    return topValue_;
  }

  protected void setTopValue(Value value) {
    topValue_ = new Value(value);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment environment) {
    MaskingIterator iterator = create();
    iterator.init(source_.deepCopy(environment), options_, environment);
    return iterator;
  }

  /**
   * Create a new instance of the current class.
   *
   * @return a new instance of the current class overriding {@link MaskingIterator}.
   */
  protected abstract MaskingIterator create();

  /**
   * Create a new (Key, Value) pair based on the current one. Must call setTopKey() and
   * setTopValue(). Must preserve sort order.
   *
   * @param key current Key.
   * @param value current Value.
   */
  protected abstract void setTopKeyValue(Key key, Value value);

  /**
   * Get the user's salt.
   *
   * @return the user's salt.
   */
  protected String salt() {
    return options_.getOrDefault(SALT_CRITERION, null);
  }

  /**
   * Get the user's authorizations.
   *
   * @return the user's authorizations.
   */
  protected Set<String> parsedAuths() {
    if (auths_ == null) {
      auths_ = Sets.newHashSet(
          Splitter.on(',').trimResults().omitEmptyStrings().split(options_.get(AUTHS_CRITERION)));
    }
    return new HashSet<>(auths_);
  }

  /**
   * Create a {@link VisibilityEvaluator} from the user auths.
   *
   * @param auths the user auths.
   * @return a {@link VisibilityEvaluator}.
   */
  protected VisibilityEvaluator visibilityEvaluator(String auths) {
    return visibilityEvaluator(Users.authorizations(auths));
  }

  /**
   * Create a {@link VisibilityEvaluator} from the user auths.
   *
   * @param auths the user auths.
   * @return a {@link VisibilityEvaluator}.
   */
  protected VisibilityEvaluator visibilityEvaluator(Set<String> auths) {
    return visibilityEvaluator(Users.authorizations(auths));
  }

  /**
   * Create a {@link VisibilityEvaluator} from the user auths.
   *
   * @param auths the user auths.
   * @return a {@link VisibilityEvaluator}.
   */
  protected VisibilityEvaluator visibilityEvaluator(Authorizations auths) {
    return new VisibilityEvaluator(auths);
  }

  /**
   * Check if the current row visibility labels match the provided auths.
   *
   * @param visibilityEvaluator the user auths ready to be evaluated against the row visibility
   *        labels.
   * @param visibility the row visibility labels.
   * @return true if the row visibility labels match the provided auths, false otherwise.
   */
  protected boolean matches(VisibilityEvaluator visibilityEvaluator, ColumnVisibility visibility) {
    try {
      return visibilityEvaluator != null && visibilityEvaluator.evaluate(visibility);
    } catch (VisibilityParseException e) {
      return false;
    }
  }
}

