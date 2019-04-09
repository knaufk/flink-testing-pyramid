package com.github.knaufk.udfs;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A simple {@link AssignerWithPeriodicWatermarks} similar to {@link
 * BoundedOutOfOrdernessTimestampExtractor}, which takes the existing timestamp of the {@link
 * StreamRecord} instead of extracting a new timestamp.
 *
 * @param <T>
 */
public class BoundedOutOfOrdernessWatermarkAssigner<T>
    implements AssignerWithPeriodicWatermarks<T> {
  private static final long serialVersionUID = 1L;

  /** The current maximum timestamp seen so far. */
  private long currentMaxTimestamp;

  /** The timestamp of the last emitted watermark. */
  private long lastEmittedWatermark = Long.MIN_VALUE;

  /**
   * The (fixed) interval between the maximum seen timestamp seen in the records and that of the
   * watermark to be emitted.
   */
  private final long maxOutOfOrderness;

  public BoundedOutOfOrdernessWatermarkAssigner(Time maxOutOfOrderness) {
    if (maxOutOfOrderness.toMilliseconds() < 0) {
      throw new RuntimeException(
          "Tried to set the maximum allowed "
              + "lateness to "
              + maxOutOfOrderness
              + ". This parameter cannot be negative.");
    }
    this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
    this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
  }

  public long getMaxOutOfOrdernessInMillis() {
    return maxOutOfOrderness;
  }

  @Override
  public final Watermark getCurrentWatermark() {
    // this guarantees that the watermark never goes backwards.
    long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
    if (potentialWM >= lastEmittedWatermark) {
      lastEmittedWatermark = potentialWM;
    }
    return new Watermark(lastEmittedWatermark);
  }

  @Override
  public final long extractTimestamp(T element, long previousElementTimestamp) {
    if (previousElementTimestamp > currentMaxTimestamp) {
      currentMaxTimestamp = previousElementTimestamp;
    }
    return previousElementTimestamp;
  }
}
