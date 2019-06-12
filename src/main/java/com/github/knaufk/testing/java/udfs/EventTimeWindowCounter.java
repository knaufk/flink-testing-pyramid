package com.github.knaufk.testing.java.udfs;

import java.util.Iterator;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link KeyedProcessFunction}, which for each key counts the number of events within a tumbling
 * event time window of the size given in the constructor. The counts is emitted once, when the
 * watermark passes the end time of the window.
 */
public class EventTimeWindowCounter
    extends KeyedProcessFunction<Integer, Integer, Tuple3<Long, Integer, Integer>> {

  private long windowSizeMs;
  private MapState<Long, Integer> windowCounts;

  public EventTimeWindowCounter(Time windowSize) {
    this.windowSizeMs = windowSize.toMilliseconds();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    windowCounts =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<Long, Integer>("windowCounts", Long.class, Integer.class));
  }

  @Override
  public void processElement(
      Integer value, Context ctx, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
    long windowStart = getWindowStart(ctx.timestamp(), windowSizeMs);

    Integer currentCount = windowCounts.get(windowStart);
    if (currentCount == null) {
      windowCounts.put(windowStart, 1);
      ctx.timerService().registerEventTimeTimer(windowStart + windowSizeMs);
    } else {
      windowCounts.put(windowStart, currentCount + 1);
    }
  }

  private long getWindowStart(long timestamp, long windowSize) {
    return (timestamp / windowSize) * windowSize;
  }

  @Override
  public void onTimer(
      long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Integer, Integer>> out)
      throws Exception {

    // Iterate over all windows to find the windows to fire
    Iterator<Long> iterator = windowCounts.keys().iterator();

    while (iterator.hasNext()) {
      Long windowStartTime = iterator.next();
      long windowEndTime = windowStartTime + windowSizeMs;
      if (windowEndTime <= ctx.timestamp()) {
        out.collect(
            new Tuple3<>(windowEndTime, ctx.getCurrentKey(), windowCounts.get(windowStartTime)));
        iterator.remove();
      }
    }
  }
}
