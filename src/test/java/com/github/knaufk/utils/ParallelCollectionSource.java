package com.github.knaufk.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * A parallel, finite/bounded source function, which emits the records given to it during construction. The records are
 * emitted with a timestamp, which is also given during construction.
 *
 */
public class ParallelCollectionSource<T> extends RichParallelSourceFunction<T> {

  private List<Tuple2<Long, T>> input;
  private List<Tuple2<Long, T>> inputOfSubtask;

  public ParallelCollectionSource(List<Tuple2<Long, T>> input) {
    this.input = input;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

    inputOfSubtask = new ArrayList<>();

    for (int i = 0; i < input.size(); i++) {
      if (i % numberOfParallelSubtasks == indexOfThisSubtask) {
        inputOfSubtask.add(input.get(i));
      }
    }
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    for (Tuple2<Long, T> integerListWithTimestamp : inputOfSubtask) {
      ctx.collectWithTimestamp(integerListWithTimestamp.f1, integerListWithTimestamp.f0);
    }
  }

  @Override
  public void cancel() {
    // ignore cancel, finite anyway
  }
}
