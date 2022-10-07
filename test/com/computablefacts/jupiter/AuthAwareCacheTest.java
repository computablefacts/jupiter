package com.computablefacts.jupiter;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.AbstractMap;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

public class AuthAwareCacheTest {

  @Test
  public void testGetIfPresent() {

    AuthAwareCache cache = new AuthAwareCache(CacheBuilder.newBuilder().build());
    cache.put("test", "key1", new ColumnVisibility("AUTH1"), "value1");
    cache.put("test", "key1", new ColumnVisibility("AUTH2"), "value2");
    cache.put("test", "key1", new ColumnVisibility("AUTH3"), "value3");
    cache.put("test", "key2", new ColumnVisibility("AUTH1|AUTH2"), "value4");
    cache.put("test", "key3", null, "value5");

    // Test viz vs no auths
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key1"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2"));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key4"));

    // Test viz vs auths
    Assert.assertEquals(ImmutableList.of("value1"), cache.getIfPresent("test", "key1", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value2"), cache.getIfPresent("test", "key1", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value3"), cache.getIfPresent("test", "key1", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value1", "value2", "value3"),
        cache.getIfPresent("test", "key1", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value4"),
        cache.getIfPresent("test", "key2", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value5"),
        cache.getIfPresent("test", "key3", new Authorizations("AUTH1", "AUTH2", "AUTH3")));
  }

  @Test
  public void testGetAllPresent() {

    AuthAwareCache cache = new AuthAwareCache(CacheBuilder.newBuilder().build());
    cache.put("test", "key1", new ColumnVisibility("AUTH1"), "value1");
    cache.put("test", "key1", new ColumnVisibility("AUTH2"), "value2");
    cache.put("test", "key1", new ColumnVisibility("AUTH3"), "value3");
    cache.put("test", "key2", new ColumnVisibility("AUTH1|AUTH2"), "value4");
    cache.put("test", "key3", null, "value5");

    // Test viz vs no auths
    Assert.assertEquals(ImmutableList.of(new AbstractMap.SimpleImmutableEntry<>("key3", "value5")),
        cache.getAllPresent("test", Sets.newHashSet("key1", "key2", "key3")));

    // Test viz vs auths
    Assert.assertEquals(Sets.newHashSet(new AbstractMap.SimpleImmutableEntry<>("key1", "value1"),
        new AbstractMap.SimpleImmutableEntry<>("key2", "value4"),
        new AbstractMap.SimpleImmutableEntry<>("key3", "value5")), Sets.newHashSet(
        cache.getAllPresent("test", Sets.newHashSet("key1", "key2", "key3"), new Authorizations("AUTH1"))));

    Assert.assertEquals(Sets.newHashSet(new AbstractMap.SimpleImmutableEntry<>("key1", "value2"),
        new AbstractMap.SimpleImmutableEntry<>("key2", "value4"),
        new AbstractMap.SimpleImmutableEntry<>("key3", "value5")), Sets.newHashSet(
        cache.getAllPresent("test", Sets.newHashSet("key1", "key2", "key3"), new Authorizations("AUTH2"))));

    Assert.assertEquals(Sets.newHashSet(new AbstractMap.SimpleImmutableEntry<>("key1", "value3"),
        new AbstractMap.SimpleImmutableEntry<>("key3", "value5")), Sets.newHashSet(
        cache.getAllPresent("test", Sets.newHashSet("key1", "key2", "key3"), new Authorizations("AUTH3"))));

    Assert.assertEquals(Sets.newHashSet(new AbstractMap.SimpleImmutableEntry<>("key1", "value1"),
        new AbstractMap.SimpleImmutableEntry<>("key1", "value2"),
        new AbstractMap.SimpleImmutableEntry<>("key1", "value3"),
        new AbstractMap.SimpleImmutableEntry<>("key2", "value4"),
        new AbstractMap.SimpleImmutableEntry<>("key3", "value5")), Sets.newHashSet(
        cache.getAllPresent("test", Sets.newHashSet("key1", "key2", "key3"),
            new Authorizations("AUTH1", "AUTH2", "AUTH3"))));
  }

  @Test
  public void testGetWithoutAuths() {

    AuthAwareCache cache = new AuthAwareCache(CacheBuilder.newBuilder().build());

    ImmutableList<String> l1 = cache.get("test", "key1",
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH1"), "value1"));

    Assert.assertEquals(ImmutableList.of(), l1);

    ImmutableList<String> l2 = cache.get("test", "key1",
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH2"), "value2"));

    Assert.assertEquals(ImmutableList.of(), l2);

    ImmutableList<String> l3 = cache.get("test", "key1",
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH3"), "value3"));

    Assert.assertEquals(ImmutableList.of(), l3);

    ImmutableList<String> l4 = cache.get("test", "key2",
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH1|AUTH2"), "value4"));

    Assert.assertEquals(ImmutableList.of(), l4);

    ImmutableList<String> l5 = cache.get("test", "key3",
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility(), "value5"));

    Assert.assertEquals(ImmutableList.of("value5"), l5);

    // Test viz vs no auths
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key1"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2"));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key4"));

    // Test viz vs auths
    Assert.assertEquals(ImmutableList.of("value1"), cache.getIfPresent("test", "key1", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value2"), cache.getIfPresent("test", "key1", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value3"), cache.getIfPresent("test", "key1", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value1", "value2", "value3"),
        cache.getIfPresent("test", "key1", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value4"),
        cache.getIfPresent("test", "key2", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value5"),
        cache.getIfPresent("test", "key3", new Authorizations("AUTH1", "AUTH2", "AUTH3")));
  }

  @Test
  public void testGetWithAuths() {

    AuthAwareCache cache = new AuthAwareCache(CacheBuilder.newBuilder().build());

    ImmutableList<String> l1 = cache.get("test", "key1", new Authorizations("AUTH1"),
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH1"), "value1"));

    Assert.assertEquals(ImmutableList.of("value1"), l1);

    ImmutableList<String> l2 = cache.get("test", "key1", new Authorizations("AUTH2"),
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH2"), "value2"));

    Assert.assertEquals(ImmutableList.of("value2"), l2);

    ImmutableList<String> l3 = cache.get("test", "key1", new Authorizations("AUTH3"),
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH3"), "value3"));

    Assert.assertEquals(ImmutableList.of("value3"), l3);

    ImmutableList<String> l4 = cache.get("test", "key2", new Authorizations("AUTH1", "AUTH2"),
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility("AUTH1|AUTH2"), "value4"));

    Assert.assertEquals(ImmutableList.of("value4"), l4);

    ImmutableList<String> l5 = cache.get("test", "key3", Authorizations.EMPTY,
        () -> new AbstractMap.SimpleImmutableEntry<>(new ColumnVisibility(), "value5"));

    Assert.assertEquals(ImmutableList.of("value5"), l5);

    // Test viz vs no auths
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key1"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2"));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3"));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key4"));

    // Test viz vs auths
    Assert.assertEquals(ImmutableList.of("value1"), cache.getIfPresent("test", "key1", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value2"), cache.getIfPresent("test", "key1", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value3"), cache.getIfPresent("test", "key1", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value1", "value2", "value3"),
        cache.getIfPresent("test", "key1", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value4"), cache.getIfPresent("test", "key2", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of(), cache.getIfPresent("test", "key2", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value4"),
        cache.getIfPresent("test", "key2", new Authorizations("AUTH1", "AUTH2", "AUTH3")));

    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH1")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH2")));
    Assert.assertEquals(ImmutableList.of("value5"), cache.getIfPresent("test", "key3", new Authorizations("AUTH3")));
    Assert.assertEquals(ImmutableList.of("value5"),
        cache.getIfPresent("test", "key3", new Authorizations("AUTH1", "AUTH2", "AUTH3")));
  }
}
