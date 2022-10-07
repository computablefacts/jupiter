package com.computablefacts.jupiter;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_NUL;

import com.computablefacts.asterix.Generated;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An application cache that takes into account both the user authorizations and the data visibility labels.
 */
@CheckReturnValue
public final class AuthAwareCache {

  private static final Logger logger_ = LoggerFactory.getLogger(AuthAwareCache.class);
  private final com.google.common.cache.Cache<String, CacheEntry<?>> cache_;

  public AuthAwareCache(com.google.common.cache.Cache<String, CacheEntry<?>> cache) {
    cache_ = Preconditions.checkNotNull(cache, "cache should not be null");
  }

  public <T> ImmutableList<T> getIfPresent(String namespace, String key) {
    return getIfPresent(namespace, key, null);
  }

  public <T> ImmutableList<T> getIfPresent(String namespace, String key, Authorizations auths) {

    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(key, "key should not be null");

    CacheEntry<T> t = (CacheEntry<T>) cache_.getIfPresent(cacheKey(namespace, key));
    return t == null ? ImmutableList.of() : t.get(auths);
  }

  public <T> ImmutableList<Map.Entry<String, T>> getAllPresent(String namespace, Set<String> keys) {
    return getAllPresent(namespace, keys, null);
  }

  public <T> ImmutableList<Map.Entry<String, T>> getAllPresent(String namespace, Set<String> keys,
      Authorizations auths) {

    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(keys, "keys should not be null");

    return ImmutableList.copyOf(
        cache_.getAllPresent(keys.stream().map(k -> cacheKey(namespace, k)).collect(Collectors.toSet())).entrySet()
            .stream().flatMap(entry -> entry.getValue().get(auths).stream().map(
                e -> new AbstractMap.SimpleImmutableEntry<>(
                    entry.getKey().substring(namespace.length() + 1 /* SEPARATOR_NUL */), (T) e)))
            .collect(Collectors.toList()));
  }

  public <T> ImmutableList<T> get(String namespace, String key, Callable<Map.Entry<ColumnVisibility, T>> callable) {
    return get(namespace, key, null, callable);
  }

  public <T> ImmutableList<T> get(String namespace, String key, Authorizations auths,
      Callable<Map.Entry<ColumnVisibility, T>> callable) {

    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(callable, "callable should not be null");

    try {
      @Var CacheEntry<T> cacheEntry = (CacheEntry<T>) cache_.getIfPresent(cacheKey(namespace, key));
      if (cacheEntry == null) {
        cacheEntry = (CacheEntry<T>) cache_.get(cacheKey(namespace, key), () -> {
          Map.Entry<ColumnVisibility, T> entry = callable.call();
          CacheEntry<T> newCacheEntry = new CacheEntry<>();
          newCacheEntry.put(entry.getKey(), entry.getValue());
          return newCacheEntry;
        });
        return cacheEntry.get(auths);
      }
      if (cacheEntry.get(auths).isEmpty()) {
        Map.Entry<ColumnVisibility, T> entry = callable.call();
        cacheEntry.put(entry.getKey(), entry.getValue());
      }
      return cacheEntry.get(auths);
    } catch (Exception e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return ImmutableList.of();
  }

  public <T> void put(String namespace, String key, ColumnVisibility cv, T value) {

    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(key, "key should not be null");

    @Var CacheEntry<T> cacheEntry = (CacheEntry<T>) cache_.getIfPresent(cacheKey(namespace, key));
    if (cacheEntry == null) {
      cacheEntry = new CacheEntry<>();
      cache_.put(cacheKey(namespace, key), cacheEntry);
    }
    cacheEntry.put(cv, value);
  }

  @Generated
  public void invalidateAll() {
    cache_.invalidateAll();
  }

  @Generated
  public void logStats() {
    if (logger_.isInfoEnabled()) {
      CacheStats stats = cache_.stats();
      logger_.info(
          LogFormatter.create(true).add("request_count", stats.requestCount()).add("hit_count", stats.hitCount())
              .add("miss_count", stats.missCount()).add("hit_rate", stats.hitRate()).add("miss_rate", stats.missRate())
              .add("load_success_count", stats.loadSuccessCount())
              .add("load_exception_count", stats.loadExceptionCount())
              .add("load_exception_rate", stats.loadExceptionRate()).add("total_load_time", stats.totalLoadTime())
              .add("average_load_penalty", stats.averageLoadPenalty()).add("eviction_count", stats.evictionCount())
              .formatInfo());
    }
  }

  private String cacheKey(String namespace, String key) {

    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(key, "key should not be null");

    return namespace + SEPARATOR_NUL + key;
  }

  @CheckReturnValue
  final public static class CacheEntry<T> {

    private final Map<ColumnVisibility, T> entries_ = new ConcurrentHashMap<>();

    public CacheEntry() {
    }

    public void put(ColumnVisibility cv, T t) {

      Preconditions.checkNotNull(t, "t should not be null");

      entries_.put(cv == null ? new ColumnVisibility() : cv, t);
    }

    public ImmutableList<T> get(Authorizations auths) {
      if (auths == null) {
        VisibilityEvaluator ve = new VisibilityEvaluator(Authorizations.EMPTY);
        return ImmutableList.copyOf(
            entries_.entrySet().stream().filter(entry -> isVisible(ve, entry.getKey())).map(entry -> entry.getValue())
                .collect(Collectors.toList()));
      }
      VisibilityEvaluator ve = new VisibilityEvaluator(auths);
      return ImmutableList.copyOf(
          entries_.entrySet().stream().filter(entry -> isVisible(ve, entry.getKey())).map(entry -> entry.getValue())
              .collect(Collectors.toList()));
    }

    private boolean isVisible(VisibilityEvaluator ve, ColumnVisibility cv) {
      try {
        return ve.evaluate(cv);
      } catch (VisibilityParseException e) {
        return false;
      }
    }
  }
}
