package com.computablefacts.jupiter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.logfmt.LogFormatter;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Mutations {

  private static final Logger logger_ = LoggerFactory.getLogger(Mutations.class);

  private Mutations() {}

  /**
   * Serialize a mutation to a byte array.
   *
   * @param mutation a {@link Mutation}.
   * @return a byte array.
   */
  @Beta
  public static byte[] serialize(Mutation mutation) {

    Preconditions.checkNotNull(mutation, "mutation should not be null");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    try (DataOutputStream dataOutputStream = new DataOutputStream(bytes)) {
      mutation.write(dataOutputStream);
    } catch (IOException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return bytes.toByteArray();
  }

  /**
   * Deserialize a byte array to a mutation.
   *
   * @param bytes a byte array.
   * @return a {@link Mutation}.
   */
  @Beta
  public static Mutation deserialize(byte[] bytes) {

    Preconditions.checkNotNull(bytes, "bytes should not be null");

    Mutation mutation = new Mutation();

    try (DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
      mutation.readFields(dataInputStream);
    } catch (IOException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return mutation;
  }
}
