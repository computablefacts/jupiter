package com.computablefacts.jupiter;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class MiniAccumuloClusterTest {

  private static final AtomicInteger tableId_ = new AtomicInteger(0);
  private static final AtomicInteger userId_ = new AtomicInteger(0);
  private static ThreadLocal<MiniAccumuloCluster> accumulo_;

  @BeforeClass
  public static void initClass() {
    accumulo_ = ThreadLocal.withInitial(() -> {
      try {
        return MiniAccumuloClusterUtils.newCluster();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    });
  }

  @AfterClass
  public static void uinitClass() throws Exception {
    MiniAccumuloClusterUtils.destroyCluster(accumulo_.get());
  }

  public MiniAccumuloCluster accumulo() {
    return accumulo_.get();
  }

  public String nextTableName() {
    return "table_" + tableId_.incrementAndGet();
  }

  public String nextUsername() {
    return "user_" + userId_.incrementAndGet();
  }
}
