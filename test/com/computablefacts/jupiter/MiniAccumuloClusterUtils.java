package com.computablefacts.jupiter;

import static org.apache.accumulo.minicluster.MemoryUnit.MEGABYTE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;

import com.computablefacts.jupiter.storage.Constants;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class MiniAccumuloClusterUtils {

  public static final String MAC_PASSWORD = "secret";
  public static final String MAC_USER = "root";

  private MiniAccumuloClusterUtils() {}

  public static boolean isWindows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  public static Configurations newConfiguration(MiniAccumuloCluster accumulo) {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");

    return newConfiguration(accumulo, MiniAccumuloClusterUtils.MAC_USER,
        MiniAccumuloClusterUtils.MAC_PASSWORD);
  }

  public static Configurations newConfiguration(MiniAccumuloCluster accumulo, String username) {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");
    Preconditions.checkNotNull(username, "username should not be null");

    return newConfiguration(accumulo, username, username);
  }

  public static Configurations newConfiguration(MiniAccumuloCluster accumulo, String username,
      String password) {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");
    Preconditions.checkNotNull(username, "username should not be null");
    Preconditions.checkNotNull(password, "password should not be null");

    return new Configurations(accumulo.getInstanceName(), accumulo.getZooKeepers(), username,
        password);
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster newUser(MiniAccumuloCluster accumulo, String user)
      throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");

    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations().createLocalUser(user,
        new PasswordToken(user));

    return accumulo;
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster setUserAuths(MiniAccumuloCluster accumulo, Authorizations auths)
      throws Exception {
    return setUserAuths(accumulo, MAC_USER, auths);
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster setUserAuths(MiniAccumuloCluster accumulo, String username,
      Authorizations auths) throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");
    Preconditions.checkNotNull(username, "username should not be null");

    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .changeUserAuthorizations(username, auths);

    return accumulo;
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster setUserTablePermissions(MiniAccumuloCluster accumulo,
      String username, String table) throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");
    Preconditions.checkNotNull(username, "username should not be null");
    Preconditions.checkNotNull(table, "table should not be null");

    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .grantTablePermission(username, table, TablePermission.READ);
    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .grantTablePermission(username, table, TablePermission.WRITE);

    return accumulo;
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster setUserSystemPermissions(MiniAccumuloCluster accumulo,
      String username) throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");
    Preconditions.checkNotNull(username, "username should not be null");

    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .grantSystemPermission(username, SystemPermission.CREATE_TABLE);
    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .grantSystemPermission(username, SystemPermission.DROP_TABLE);

    return accumulo;
  }

  /**
   * Creates and starts an instance of MiniAccumuloCluster, returning the new instance.
   *
   * @return a new {@link MiniAccumuloCluster}.
   */
  public static MiniAccumuloCluster newCluster() throws Exception {

    // Create MAC directory
    File macDir = Files.createTempDirectory("mac-").toFile();

    // Set cluster configuration
    MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, MAC_PASSWORD);
    config.setDefaultMemory(512, MEGABYTE);

    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);

    if (isWindows()) {

      Preconditions.checkState(new File("C:\\a_bin\\winutils-master\\hadoop-2.8.3").exists());

      // See https://issues.apache.org/jira/browse/ACCUMULO-3293 for details
      File binDir = new File(macDir.getAbsolutePath() + "/bin");

      if (!binDir.exists()) {
        binDir.mkdirs();
      }

      try (
          Stream<Path> stream = Files.walk(Paths.get("C:\\a_bin\\winutils-master\\hadoop-2.8.3"))) {
        stream.forEach(file -> {
          try {
            Files.copy(file, Paths.get(macDir.getAbsolutePath() + "/bin/" + file.getFileName()),
                StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            // TODO
          }
        });
      }
    }

    // Start MAC and connect to it
    accumulo.start();

    // Add shutdown hook to stop MAC and cleanup temporary files
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        accumulo.stop();
      } catch (IOException | InterruptedException e) {
        Thread.currentThread().interrupt();
        // throw new InterruptedException("Failed to shut down MAC instance", e);
      }
      try {
        FileUtils.forceDelete(macDir);
      } catch (IOException e) {
        // throw new IOException("Failed to clean up MAC directory", e);
      }
    }));
    return setUserAuths(accumulo, Constants.AUTH_ADM);
  }

  /**
   * Destroy an existing MiniAccumuloCluster. Force removal of the underlying MAC directory.
   *
   * @param accumulo a {@link MiniAccumuloCluster}.
   */
  public static void destroyCluster(MiniAccumuloCluster accumulo) throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");

    accumulo.stop();
    FileUtils.forceDelete(accumulo.getConfig().getDir());
  }
}
