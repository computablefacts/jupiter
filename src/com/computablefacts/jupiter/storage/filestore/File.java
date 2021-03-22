package com.computablefacts.jupiter.storage.filestore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.computablefacts.nona.Generated;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class File {

  private final String key_;
  private final Set<String> labels_;
  private final String filename_;
  private final long fileSize_;
  private final byte[] fileContent_;

  public File(String key, Set<String> labels, String filename, long fileSize, byte[] fileContent) {

    Preconditions.checkNotNull(key, "key should not be null");
    Preconditions.checkNotNull(labels, "labels should not be null");
    Preconditions.checkNotNull(filename, "filename should not be null");
    Preconditions.checkNotNull(fileContent, "fileContent should not be null");
    Preconditions.checkArgument(fileSize >= 0, "fileSize should be >= 0");

    key_ = key;
    labels_ = new HashSet<>(labels);
    filename_ = filename;
    fileSize_ = fileSize;
    fileContent_ = fileContent;
  }

  @Generated
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key_).add("labels", labels_)
        .add("filename", filename_).add("file_size", fileSize_).add("file_content", fileContent_)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof File)) {
      return false;
    }
    File file = (File) obj;
    return Objects.equal(key_, file.key_) && Objects.equal(labels_, file.labels_)
        && Objects.equal(filename_, file.filename_) && Objects.equal(fileSize_, file.fileSize_)
        && Arrays.equals(fileContent_, file.fileContent_);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key_, labels_, filename_, fileSize_, Arrays.hashCode(fileContent_));
  }

  @Generated
  public String key() {
    return key_;
  }

  @Generated
  public Set<String> labels() {
    return labels_;
  }

  @Generated
  public String filename() {
    return filename_;
  }

  @Generated
  public long fileSize() {
    return fileSize_;
  }

  @Generated
  public byte[] fileContent() {
    return fileContent_;
  }
}
