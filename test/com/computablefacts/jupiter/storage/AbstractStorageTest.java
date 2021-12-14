package com.computablefacts.jupiter.storage;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.computablefacts.jupiter.MiniAccumuloClusterTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AbstractStorageTest extends MiniAccumuloClusterTest {

  @Test(expected = NullPointerException.class)
  public void testEncodeNull() {
    String str = AbstractStorage.encode(null);
  }

  @Test
  public void testEncode() {
    Assert.assertEquals("My\\u0000message\\u0000!", AbstractStorage.encode("My\0message\0!"));
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNull() {
    String str = AbstractStorage.decode(null);
  }

  @Test
  public void testDecode() {
    Assert.assertEquals("My\0message\0!", AbstractStorage.decode("My\\u0000message\\u0000!"));
  }

  @Test(expected = NullPointerException.class)
  public void testToVisibilityLabelNull() {
    String str = AbstractStorage.toVisibilityLabel(null);
  }

  @Test
  public void testToVisibilityLabel() {
    Assert.assertEquals("2001_0DB8_0001_0000_0000_0AB9_C0A8_0102",
        AbstractStorage.toVisibilityLabel("2001:0db8:0001:0000:0000:0ab9:C0A8:0102"));
    Assert.assertEquals("000_0000_00_00T00_00_00_000Z",
        AbstractStorage.toVisibilityLabel("000|0000-00-00T00:00:00.000Z"));
    Assert.assertEquals("ACTORS_CHILDREN_NAME",
        AbstractStorage.toVisibilityLabel("actors[0]¤children[1].name"));
  }

  @Test(expected = NullPointerException.class)
  public void testToVisibilityLabelsNull() {
    Set<String> viz = AbstractStorage.toVisibilityLabels(null);
  }

  @Test
  public void testToVisibilityLabels() {
    Assert.assertEquals(Sets.newHashSet("DATA"),
        AbstractStorage.toVisibilityLabels(Lists.newArrayList("data")));
    Assert.assertEquals(Sets.newHashSet("DATA", "DATA_USER"),
        AbstractStorage.toVisibilityLabels(Lists.newArrayList("data", "user")));
    Assert.assertEquals(Sets.newHashSet("DATA", "DATA_USER", "DATA_USER_USERNAME"),
        AbstractStorage.toVisibilityLabels(Lists.newArrayList("data", "user", "username")));
    Assert.assertEquals(Sets.newHashSet("DATA", "DATA_USER", "DATA_USER_USERNAME"),
        AbstractStorage.toVisibilityLabels(Lists.newArrayList("data", "user", "username", "raw")));
    Assert.assertEquals(Sets.newHashSet("ACTORS", "ACTORS_CHILDREN", "ACTORS_CHILDREN_NAME"),
        AbstractStorage.toVisibilityLabels(Lists.newArrayList("actors[0]", "children[1]", "name")));
  }

  @Test
  public void testNullToEmpty() {
    Assert.assertEquals(Authorizations.EMPTY, AbstractStorage.nullToEmpty(null));
    Assert.assertEquals(Constants.AUTH_ADM, AbstractStorage.nullToEmpty(Constants.AUTH_ADM));
  }

  @Test
  public void testCompactWithoutAuths() {
    Assert.assertEquals(Authorizations.EMPTY, AbstractStorage.compact(null, null, null));
  }

  @Test
  public void testCompactWithoutPrefixWithoutSuffix() {
    Assert.assertEquals(new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT"),
        AbstractStorage.compact(new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT"), null, null));
  }

  @Test
  public void testCompactWithPrefixWithoutSuffix() {

    Assert.assertEquals(new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT"),
        AbstractStorage.compact(new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT", "DS2_CONTENT",
            "DS2_CONTENT_TEXT"), "ds1", null));

    Assert.assertEquals(new Authorizations(Constants.STRING_ADM, "DS2_CONTENT", "DS2_CONTENT_TEXT"),
        AbstractStorage.compact(new Authorizations(Constants.STRING_ADM, "DS1_CONTENT",
            "DS1_CONTENT_TEXT", "DS2_CONTENT", "DS2_CONTENT_TEXT"), "ds2", null));
  }

  @Test
  public void testCompactWithoutPrefixWithSuffix() {

    Assert.assertEquals(new Authorizations("DS1_CONTENT", "DS2_CONTENT"), AbstractStorage.compact(
        new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT", "DS2_CONTENT", "DS2_CONTENT_TEXT"),
        null, "CONTENT"));

    Assert.assertEquals(new Authorizations(Constants.STRING_ADM, "DS1_CONTENT", "DS2_CONTENT"),
        AbstractStorage.compact(new Authorizations(Constants.STRING_ADM, "DS1_CONTENT",
            "DS1_CONTENT_TEXT", "DS2_CONTENT", "DS2_CONTENT_TEXT"), null, "CONTENT"));
  }

  @Test
  public void testCompactWithPrefixWithSuffix() {

    Assert.assertEquals(new Authorizations("DS1_CONTENT"), AbstractStorage.compact(
        new Authorizations("DS1_CONTENT", "DS1_CONTENT_TEXT", "DS2_CONTENT", "DS2_CONTENT_TEXT"),
        "ds1", "CONTENT"));

    Assert.assertEquals(new Authorizations(Constants.STRING_ADM, "DS1_CONTENT"),
        AbstractStorage.compact(new Authorizations(Constants.STRING_ADM, "DS1_CONTENT",
            "DS1_CONTENT_TEXT", "DS2_CONTENT", "DS2_CONTENT_TEXT"), "ds1", "CONTENT"));
  }
}
