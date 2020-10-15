package com.computablefacts.jupiter;

import static org.apache.accumulo.minicluster.MemoryUnit.MEGABYTE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.accumulo.core.security.Authorizations;
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

    return new Configurations(accumulo.getInstanceName(), accumulo.getZooKeepers(),
        MiniAccumuloClusterUtils.MAC_USER, MiniAccumuloClusterUtils.MAC_PASSWORD);
  }

  @CanIgnoreReturnValue
  public static MiniAccumuloCluster setUserAuths(MiniAccumuloCluster accumulo, Authorizations auths)
      throws Exception {

    Preconditions.checkNotNull(accumulo, "accumulo should not be null");

    accumulo.getConnector(MAC_USER, MAC_PASSWORD).securityOperations()
        .changeUserAuthorizations(MAC_USER, auths);

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

      Files.walk(Paths.get("C:\\a_bin\\winutils-master\\hadoop-2.8.3")).filter(Files::isRegularFile)
          .forEach(file -> {
            try {
              Files.copy(file, Paths.get(macDir.getAbsolutePath() + "/bin/" + file.getFileName()),
                  StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
              // TODO
            }
          });
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
