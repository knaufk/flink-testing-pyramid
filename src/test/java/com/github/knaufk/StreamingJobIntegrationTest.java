package com.github.knaufk;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class StreamingJobIntegrationTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Test
  public void testCompletePipeline() throws Exception {

    // Arrange
    CollectingSink sink = new CollectingSink();
    ParallelSourceFunction source =
        new ParallelCollectionSource(
            Arrays.asList(
                new Tuple2<>(500L, Arrays.asList(1)),
                new Tuple2<>(1000L, Arrays.asList(1, 2, 3)),
                new Tuple2<>(1500L, Arrays.asList(1, 2)),
                new Tuple2<>(1999L, Arrays.asList(3)),
                new Tuple2<>(2000L, Arrays.asList(1, 2, 3, 1, 2, 3)),
                new Tuple2<>(2100L, Arrays.asList(1))));
    StreamingJob job = new StreamingJob(source, sink);

    // Act
    job.execute();

    // Assert
    // Long.MAX_VALUE watermark is sent at the end of finite source
    assertThat(sink.result)
        .containsExactlyInAnyOrder(
            new Tuple3<>(1000L, 1, 1),
            new Tuple3<>(2000L, 1, 2),
            new Tuple3<>(2000L, 2, 2),
            new Tuple3<>(2000L, 3, 2),
            new Tuple3<>(3000L, 1, 3),
            new Tuple3<>(3000L, 2, 2),
            new Tuple3<>(3000L, 3, 2));
  }

  private static class ParallelCollectionSource extends RichParallelSourceFunction<List<Integer>> {

    private List<Tuple2<Long, List<Integer>>> input;
    private List<Tuple2<Long, List<Integer>>> inputOfSubtask;

    public ParallelCollectionSource(List<Tuple2<Long, List<Integer>>> input) {
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
    public void run(SourceContext<List<Integer>> ctx) throws Exception {
      for (Tuple2<Long, List<Integer>> integerListWithTimestamp : inputOfSubtask) {
        ctx.collectWithTimestamp(integerListWithTimestamp.f1, integerListWithTimestamp.f0);
      }
    }

    @Override
    public void cancel() {
      // ignore cancel, finite anyway
    }
  }

  private static class CollectingSink implements SinkFunction<Tuple3<Long, Integer, Integer>> {

    private static final List<Tuple3<Long, Integer, Integer>> result =
        Collections.synchronizedList(new ArrayList<>());

    public void invoke(Tuple3<Long, Integer, Integer> value, Context context) throws Exception {
      result.add(value);
    }
  }
}
