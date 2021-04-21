package com.computablefacts.jupiter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
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

  /**
   * This method differentiates between various types of exceptions we may see.
   *
   * @param exception the exception to analyze.
   * @return true if we could retry, false if we should exit.
   */
  @Beta
  public static boolean handleExceptions(Exception exception) {

    Preconditions.checkNotNull(exception, "exception should not be null");

    if (exception instanceof MutationsRejectedException) {

      // Permanent failures
      MutationsRejectedException ex = (MutationsRejectedException) exception;
      Map<TabletId, Set<SecurityErrorCode>> securityErrors = ex.getSecurityErrorCodes();

      for (Map.Entry<TabletId, Set<SecurityErrorCode>> entry : securityErrors.entrySet()) {
        for (SecurityErrorCode err : entry.getValue()) {
          logger_.error(LogFormatter.create(true).message("Permanent error: " + err.toString())
              .formatError());
        }
      }

      List<ConstraintViolationSummary> constraintViolations = ex.getConstraintViolationSummaries();

      if (!securityErrors.isEmpty() || !constraintViolations.isEmpty()) {
        logger_.error(
            LogFormatter.create(true).message("Have permanent errors. Exiting...").formatError());
        return false;
      }

      // Transient failures
      Collection<String> errorServers = ex.getErrorServers();

      for (String errorServer : errorServers) {
        logger_.warn(
            LogFormatter.create(true).message("Problem with server: " + errorServer).formatWarn());
      }

      int numUnknownExceptions = ex.getUnknownExceptions();

      if (numUnknownExceptions > 0) {
        logger_.warn(LogFormatter.create(true)
            .message(numUnknownExceptions + " unknown exceptions.").formatWarn());
      }
      return true;
    } else if (exception instanceof TimedOutException) {

      // Transient failures
      TimedOutException ex = (TimedOutException) exception;
      Collection<String> errorServers = ex.getTimedOutSevers();

      for (String errorServer : errorServers) {
        logger_.warn(LogFormatter.create(true)
            .message("Problem with server: " + errorServer + " (timeout)").formatWarn());
      }
      return true;
    }
    return false;
  }
}
