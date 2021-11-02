package com.computablefacts.asterix.trie;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.errorprone.annotations.Var;

public class TrieTest {

  private static List<OneCharacter> toCharacters(String str) {
    return Arrays.stream(str.split("(?!^)")).map(c -> new OneCharacter(c.charAt(0)))
        .collect(Collectors.toList());
  }

  @Test
  public void testTrie() {

    Trie<OneCharacter> trie = new Trie<>();
    trie.insert(toCharacters("Hello"));

    Assert.assertTrue(trie.contains(toCharacters("Hello")));

    trie.insert(toCharacters("Hello world!"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));

    trie.delete(toCharacters("Hello"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertFalse(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));

    trie.insert(toCharacters("Hello"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));

    trie.delete(toCharacters("Hello world!"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertFalse(trie.contains(toCharacters("Hello world!")));

    trie.insert(toCharacters("Hello world!"));
    trie.insert(toCharacters("Hello world."));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world.")));

    trie.delete(toCharacters("Hello world."));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));
    Assert.assertFalse(trie.contains(toCharacters("Hello world.")));

    trie.insert(toCharacters("The world is yours!"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));
    Assert.assertFalse(trie.contains(toCharacters("Hello world.")));
    Assert.assertTrue(trie.contains(toCharacters("The world is yours!")));

    trie.delete(toCharacters("The world is yours!"));

    Assert.assertFalse(trie.contains(toCharacters("world")));
    Assert.assertTrue(trie.contains(toCharacters("Hello")));
    Assert.assertTrue(trie.contains(toCharacters("Hello world!")));
    Assert.assertFalse(trie.contains(toCharacters("Hello world.")));
    Assert.assertFalse(trie.contains(toCharacters("The world is yours!")));
  }

  @Test
  public void testPaths() {

    Trie<OneCharacter> trie = new Trie<>();
    trie.insert(toCharacters("Hello!"));
    trie.insert(toCharacters("Hello world."));
    trie.insert(toCharacters("Hello world!"));
    trie.insert(toCharacters("The world is yours!"));

    @Var
    List<List<OneCharacter>> paths = trie.paths();

    Assert.assertEquals(4, paths.size());
    Assert.assertTrue(paths.contains(toCharacters("Hello!")));
    Assert.assertTrue(paths.contains(toCharacters("Hello world!")));
    Assert.assertTrue(paths.contains(toCharacters("Hello world.")));
    Assert.assertTrue(paths.contains(toCharacters("The world is yours!")));

    trie.delete(toCharacters("Hello world."));

    paths = trie.paths();

    Assert.assertEquals(3, paths.size());
    Assert.assertTrue(paths.contains(toCharacters("Hello!")));
    Assert.assertTrue(paths.contains(toCharacters("Hello world!")));
    Assert.assertFalse(paths.contains(toCharacters("Hello world.")));
    Assert.assertTrue(paths.contains(toCharacters("The world is yours!")));
  }

  @Test
  public void testRemoveIf() {

    Trie<OneCharacter> trie = new Trie<>();
    trie.insert(toCharacters("Hello!"));
    trie.insert(toCharacters("Hello world."));
    trie.insert(toCharacters("Hello world!"));
    trie.insert(toCharacters("The world is yours!"));

    @Var
    List<List<OneCharacter>> paths = trie.paths();

    Assert.assertEquals(4, paths.size());

    trie.removeIf(oc -> oc.c_ == '.');
    paths = trie.paths();

    Assert.assertEquals(3, paths.size());
    Assert.assertTrue(paths.contains(toCharacters("Hello!")));
    Assert.assertTrue(paths.contains(toCharacters("Hello world!")));
    Assert.assertFalse(paths.contains(toCharacters("Hello world.")));
    Assert.assertTrue(paths.contains(toCharacters("The world is yours!")));

    trie.removeIf(oc -> oc.c_ == 'H');
    paths = trie.paths();

    Assert.assertEquals(1, paths.size());
    Assert.assertFalse(paths.contains(toCharacters("Hello!")));
    Assert.assertFalse(paths.contains(toCharacters("Hello world!")));
    Assert.assertFalse(paths.contains(toCharacters("Hello world.")));
    Assert.assertTrue(paths.contains(toCharacters("The world is yours!")));
  }

  private static class OneCharacter {

    private final char c_;

    public OneCharacter(char c) {
      c_ = c;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof OneCharacter)) {
        return false;
      }
      OneCharacter oneCharacter = (OneCharacter) obj;
      return c_ == oneCharacter.c_;
    }

    @Override
    public int hashCode() {
      return Character.hashCode(c_);
    }
  }
}
