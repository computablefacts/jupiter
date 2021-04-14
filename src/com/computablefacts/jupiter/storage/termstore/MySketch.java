package com.computablefacts.jupiter.storage.termstore;

import java.util.List;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
final public class MySketch {

  private final UpdateSketch thetaSketch_ = UpdateSketch.builder().build();

  public MySketch() {}

  public static Sketch wrap(byte[] sketch) {
    return Sketches.wrapSketch(Memory.wrap(sketch));
  }

  public static Sketch union(List<byte[]> sketches) {
    Union union = SetOperation.builder().buildUnion();
    for (byte[] sketch : sketches) {
      union.union(wrap(sketch));
    }
    return union.getResult();
  }

  public static Sketch intersection(List<byte[]> sketches) {
    Intersection intersection = SetOperation.builder().buildIntersection();
    for (byte[] sketch : sketches) {
      intersection.intersect(wrap(sketch));
    }
    return intersection.getResult();
  }

  public void offer(String value) {
    if (thetaSketch_ != null && value != null) {
      thetaSketch_.update(value);
    }
  }

  public byte[] toByteArray() {
    return thetaSketch_ == null ? null : thetaSketch_.compact().toByteArray();
  }
}
