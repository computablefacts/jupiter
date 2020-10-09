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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.computablefacts.jupiter.logs.LogFormatterManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;

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
   * @param zooKeepers zookeepers.
   * @param username username.
   * @param password password.
   */
  public Configurations(String instanceName, String zooKeepers, String username, String password) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceName),
        "instanceName should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zooKeepers),
        "zooKeepers should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username),
        "username should neither be null nor empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(password),
        "password should neither be null nor empty");

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
        logger_.error(LogFormatterManager.logFormatter().message(e).formatError());
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
