/*
 * Copyright (c) 2011-2020 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
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

import com.computablefacts.jupiter.logs.LogFormatterManager;
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
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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
      logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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
          logger_.error(LogFormatterManager.logFormatter()
              .message("Permanent error: " + err.toString()).formatError());
        }
      }

      List<ConstraintViolationSummary> constraintViolations = ex.getConstraintViolationSummaries();

      if (!securityErrors.isEmpty() || !constraintViolations.isEmpty()) {
        logger_.error(LogFormatterManager.logFormatter()
            .message("Have permanent errors. Exiting...").formatError());
        return false;
      }

      // Transient failures
      Collection<String> errorServers = ex.getErrorServers();

      for (String errorServer : errorServers) {
        logger_.warn(LogFormatterManager.logFormatter()
            .message("Problem with server: " + errorServer).formatWarn());
      }

      int numUnknownExceptions = ex.getUnknownExceptions();

      if (numUnknownExceptions > 0) {
        logger_.warn(LogFormatterManager.logFormatter()
            .message(numUnknownExceptions + " unknown exceptions.").formatWarn());
      }
      return true;
    } else if (exception instanceof TimedOutException) {

      // Transient failures
      TimedOutException ex = (TimedOutException) exception;
      Collection<String> errorServers = ex.getTimedOutSevers();

      for (String errorServer : errorServers) {
        logger_.warn(LogFormatterManager.logFormatter()
            .message("Problem with server: " + errorServer + " (timeout)").formatWarn());
      }
      return true;
    }
    return false;
  }
}
