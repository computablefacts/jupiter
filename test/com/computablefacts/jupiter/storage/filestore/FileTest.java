package com.computablefacts.jupiter.storage.filestore;

import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FileTest {

  @Test(expected = NullPointerException.class)
  public void testNullKey() {
    File file = new File(null, Sets.newHashSet(), "file.txt", 0, new byte[0]);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    File file = new File("key", null, "file.txt", 0, new byte[0]);
  }

  @Test(expected = NullPointerException.class)
  public void testNullFilename() {
    File file = new File("key", Sets.newHashSet(), null, 0, new byte[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeFileSize() {
    File file = new File("key", Sets.newHashSet(), "file.txt", -1, new byte[0]);
  }

  @Test(expected = NullPointerException.class)
  public void testNullFileContent() {
    File file = new File("key", Sets.newHashSet(), "file.txt", 0, null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(File.class).verify();
  }
}
