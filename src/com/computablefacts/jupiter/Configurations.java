package com.computablefacts.jupiter;

import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CheckReturnValue
final public class Configurations {

  private static final Logger logger_ = LoggerFactory.getLogger(Configurations.class);

  private final String instanceName_;
  private final String zooKeepers_;
  private final String username_;
  private final String password_;

  private Instance instance_;
  private Connector connector_;
  private TableOperations tableOperations_;

  /**
   * Create a new configuration object.
   *
   * @param instanceName instance name.
   * @param zooKeepers   zookeepers.
   * @param username     username.
   * @param password     password.
   */
  public Configurations(String instanceName, String zooKeepers, String username, String password) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceName), "instanceName should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zooKeepers), "zooKeepers should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(password), "password should neither be null nor empty");

    instanceName_ = instanceName;
    zooKeepers_ = zooKeepers;
    username_ = username;
    password_ = password;
  }

  public String instanceName() {
    return instanceName_;
  }

  public String zooKeepers() {
    return zooKeepers_;
  }

  public String username() {
    return username_;
  }

  public String password() {
    return password_;
  }

  public Instance instance() {
    if (instance_ == null) {
      instance_ = new ZooKeeperInstance(instanceName_, zooKeepers_);
    }
    return instance_;
  }

  public Connector connector() {
    if (connector_ == null) {
      try {
        connector_ = instance().getConnector(username_, new PasswordToken(password_));
      } catch (AccumuloException | AccumuloSecurityException e) {
        logger_.error(LogFormatter.create(true).message(e).formatError());
      }
    }
    return connector_;
  }

  public TableOperations tableOperations() {
    if (tableOperations_ == null) {
      tableOperations_ = connector().tableOperations();
    }
    return tableOperations_;
  }
}
