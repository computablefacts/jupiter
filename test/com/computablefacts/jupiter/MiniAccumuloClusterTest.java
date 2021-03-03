package com.computablefacts.jupiter;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class MiniAccumuloClusterTest {

  private static final AtomicInteger tableId_ = new AtomicInteger(0);
  private static final AtomicInteger userId_ = new AtomicInteger(0);
  private static MiniAccumuloCluster accumulo_;

  public synchronized MiniAccumuloCluster accumulo() throws Exception {
    if (accumulo_ == null) {
      accumulo_ = MiniAccumuloClusterUtils.newCluster();
    }
    return accumulo_;
  }

  public String nextTableName() {
    return "table_" + tableId_.incrementAndGet();
  }

  public String nextUsername() {
    return "user_" + userId_.incrementAndGet();
  }
}
