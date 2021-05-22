package com.computablefacts.jupiter;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class Users {

  private static final Logger logger_ = LoggerFactory.getLogger(Users.class);

  private Users() {}

  public static ColumnVisibility columnVisibility(String viz) {
    return Strings.isNullOrEmpty(viz) ? new ColumnVisibility() : new ColumnVisibility(viz);
  }

  public static Set<String> all(Connector connector) {

    Preconditions.checkNotNull(connector, "connector should not be null");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        return securityOperations.listLocalUsers();
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return new HashSet<>();
  }

  public static boolean exists(Connector connector, String username) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");

    return all(connector).contains(username);
  }

  public static boolean create(Connector connector, String username, String password) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(password),
        "password should neither be null nor empty");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        if (!exists(connector, username)) {
          securityOperations.createLocalUser(username, new PasswordToken(password));
        }
        return true;
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }

  public static boolean delete(Connector connector, String username) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        if (exists(connector, username)) {
          securityOperations.dropLocalUser(username);
        }
        return true;
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }

  public static Authorizations authorizations(Set<String> authorizations) {
    if (authorizations == null || authorizations.isEmpty()) {
      return Authorizations.EMPTY;
    }
    return new Authorizations(Iterables.toArray(authorizations, String.class));
  }

  public static Authorizations authorizations(String authorizations) {
    if (Strings.isNullOrEmpty(authorizations)) {
      return Authorizations.EMPTY;
    }
    return new Authorizations(Iterables.toArray(
        Splitter.on(',').trimResults().omitEmptyStrings().split(authorizations), String.class));
  }

  public static Authorizations getAuthorizations(Connector connector, String username) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        return securityOperations.getUserAuthorizations(username);
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return null;
  }

  public static boolean setAuthorizations(Connector connector, String username,
      Set<String> authorizations) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkNotNull(authorizations, "authorizations should not be null");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        if (authorizations.isEmpty()) {
          securityOperations.changeUserAuthorizations(username, Authorizations.EMPTY);
        } else {
          Authorizations userAuthorizations =
              new Authorizations(authorizations.toArray(new String[authorizations.size()]));
          securityOperations.changeUserAuthorizations(username, userAuthorizations);
        }
        return true;
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }

  @Deprecated
  public static boolean grantReadPermission(Connector connector, String username, String table) {
    return grantPermission(connector, username, table, TablePermission.READ);
  }

  @Deprecated
  public static boolean grantWritePermission(Connector connector, String username, String table) {
    return grantPermission(connector, username, table, TablePermission.WRITE);
  }

  public static boolean grantPermission(Connector connector, String username, String table,
      TablePermission permission) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(table),
        "table should neither be null nor empty");
    Preconditions.checkNotNull(permission, "permission should not be null");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        securityOperations.grantTablePermission(username, table, permission);
        return true;
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }

  @Deprecated
  public static boolean revokeReadPermission(Connector connector, String username, String table) {
    return revokePermission(connector, username, table, TablePermission.READ);
  }

  @Deprecated
  public static boolean revokeWritePermission(Connector connector, String username, String table) {
    return revokePermission(connector, username, table, TablePermission.WRITE);
  }

  public static boolean revokePermission(Connector connector, String username, String table,
      TablePermission permission) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(table),
        "table should neither be null nor empty");
    Preconditions.checkNotNull(permission, "permission should not be null");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        if (hasPermission(connector, username, table, permission)) {
          securityOperations.revokeTablePermission(username, table, permission);
        }
        return true;
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }

  public static boolean hasPermission(Connector connector, String username, String table,
      TablePermission permission) {

    Preconditions.checkNotNull(connector, "connector should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(table),
        "table should neither be null nor empty");
    Preconditions.checkNotNull(permission, "permission should not be null");

    SecurityOperations securityOperations = connector.securityOperations();

    if (securityOperations != null) {
      try {
        return securityOperations.hasTablePermission(username, table, permission);
      } catch (AccumuloSecurityException | AccumuloException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return false;
  }
}
