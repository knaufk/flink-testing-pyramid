package com.github.knaufk.testing.java;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.knaufk.testing.java.utils.CollectingSink;
import com.github.knaufk.testing.java.utils.ParallelCollectionSource;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
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
    ParallelSourceFunction<List<Integer>> source =
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
}
