package com.github.knaufk;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

class IntegerListSource extends RichParallelSourceFunction<List<Integer>> {

  private volatile boolean cancelled = false;
  private Random random;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    random = new Random();
  }

  @Override
  public void run(SourceContext<List<Integer>> ctx) throws Exception {
    while (!cancelled) {
      int nextLength = random.nextInt(10);
      List<Integer> nextList = random.ints(nextLength, 0, 100).boxed().collect(Collectors.toList());
      synchronized (ctx.getCheckpointLock()) {
        ctx.collectWithTimestamp(nextList, System.currentTimeMillis());
      }
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }
}
