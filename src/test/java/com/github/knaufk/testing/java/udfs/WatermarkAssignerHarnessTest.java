package com.github.knaufk.testing.java.udfs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPeriodicWatermarksOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

public class WatermarkAssignerHarnessTest {

  private static final long ANY_TIMESTAMP = 100;
  public static final List<Integer> ANY_LIST = Arrays.asList(1);

  private OneInputStreamOperatorTestHarness<List<Integer>, List<Integer>> testHarness;
  private AssignerWithPeriodicWatermarks assigner;

  @Before
  public void setupTestHarness() throws Exception {
    assigner =
        new BoundedOutOfOrdernessWatermarkAssigner<List<Integer>>(
            Time.of(50, TimeUnit.MILLISECONDS));
    testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new TimestampsAndPeriodicWatermarksOperator<List<Integer>>(assigner));
    testHarness.getExecutionConfig().setAutoWatermarkInterval(50);
    testHarness.setProcessingTime(0L);
    testHarness.open();
  }

  @Test
  public void watermarksAreInjectedAndTimestampedAreUnchanged() throws Exception {

    // Act
    StreamRecord<List<Integer>> input100 = new StreamRecord<>(ANY_LIST, 100);
    testHarness.processElement(input100);
    testHarness.setProcessingTime(50L);
    StreamRecord<List<Integer>> input200 = new StreamRecord<>(ANY_LIST, 200);
    testHarness.processElement(input200);
    testHarness.setProcessingTime(100L);

    // Assert
    List<StreamElement> expectedOutput =
        Arrays.asList(input100, input200, new Watermark(50L), new Watermark(150));
    assertThat(testHarness.getOutput()).containsExactlyInAnyOrderElementsOf(expectedOutput);
  }
}
