package com.github.knaufk.testing.java.udfs;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

public class EvenTimeWindowCounterHarnessTest {

  private KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Tuple3<Long, Integer, Integer>>
      testHarness;
  private KeyedProcessFunction flattenFunction;

  @Before
  public void setupTestHarness() throws Exception {
    flattenFunction = new EvenTimeWindowCounter(Time.of(1, SECONDS));
    testHarness =
        new KeyedOneInputStreamOperatorTestHarness<
            Integer, Integer, Tuple3<Long, Integer, Integer>>(
            new KeyedProcessOperator<>(flattenFunction),
            integer -> integer,
            TypeInformation.of(Integer.class));
    testHarness.open();
  }

  @Test
  public void singleWindowIsEmittedOnWatermark() throws Exception {

    // Act
    testHarness.processElement(1, 400L);
    testHarness.processElement(1, 300L);
    testHarness.processElement(1, 999L);
    testHarness.processWatermark(1000L);

    // Assert
    List<StreamRecord<Tuple3<Long, Integer, Integer>>> expectedOutput =
        Arrays.asList(new StreamRecord<>(new Tuple3<>(1000L, 1, 3), 1000L));
    assertThat(testHarness.extractOutputStreamRecords())
        .containsExactlyInAnyOrderElementsOf(expectedOutput);
  }

  @Test
  public void singleWindowWithMultipleKeysIsEmittedOnWatermark() throws Exception {

    // Act
    testHarness.processElement(1, 400L);
    testHarness.processElement(1, 300L);
    testHarness.processElement(2, 200L);
    testHarness.processElement(2, 999L);
    testHarness.processWatermark(1000L);

    // Assert
    List<StreamRecord<Tuple3<Long, Integer, Integer>>> expectedOutput =
        Arrays.asList(
            new StreamRecord<>(new Tuple3<>(1000L, 1, 2), 1000L),
            new StreamRecord<>(new Tuple3<>(1000L, 2, 2), 1000L));
    assertThat(testHarness.extractOutputStreamRecords())
        .containsExactlyInAnyOrderElementsOf(expectedOutput);
  }

  @Test
  public void multipleWindowsWithMultipleKeysAreFlushedOnWatermarks() throws Exception {

    // Arrange

    // Act
    testHarness.processElement(1, 400L);
    testHarness.processElement(1, 300L);
    testHarness.processElement(2, 200L);
    testHarness.processElement(2, 999L);
    testHarness.processElement(2, 1010L);
    testHarness.processWatermark(1050L);
    testHarness.processElement(2, 1250L);
    testHarness.processElement(2, 1400L);
    testHarness.processElement(3, 1800L);
    testHarness.processWatermark(2000);

    // Assert
    List<StreamRecord<Tuple3<Long, Integer, Integer>>> expectedOutput =
        Arrays.asList(
            new StreamRecord<>(new Tuple3<>(1000L, 1, 2), 1000L),
            new StreamRecord<>(new Tuple3<>(1000L, 2, 2), 1000L),
            new StreamRecord<>(new Tuple3<>(2000L, 2, 3), 2000L),
            new StreamRecord<>(new Tuple3<>(2000L, 3, 1), 2000L));
    assertThat(testHarness.extractOutputStreamRecords())
        .containsExactlyInAnyOrderElementsOf(expectedOutput);
  }

  @Test
  public void multipleWindowTriggeredAtSameTime() throws Exception {

    // Arrange

    // Act
    testHarness.processElement(1, 400L);
    testHarness.processElement(1, 300L);
    testHarness.processElement(1, 1010L);
    testHarness.processElement(2, 1250L);
    testHarness.processElement(2, 1400L);
    testHarness.processElement(1, 1800L);
    testHarness.processElement(1, 2300L);
    testHarness.processWatermark(4010);

    // Assert
    List<StreamRecord<Tuple3<Long, Integer, Integer>>> expectedOutput =
        Arrays.asList(
            new StreamRecord<>(new Tuple3<>(1000L, 1, 2), 1000L),
            new StreamRecord<>(new Tuple3<>(2000L, 1, 2), 2000L),
            new StreamRecord<>(new Tuple3<>(2000L, 2, 2), 2000L),
            new StreamRecord<>(new Tuple3<>(3000L, 1, 1), 3000L));
    assertThat(testHarness.extractOutputStreamRecords())
        .containsExactlyInAnyOrderElementsOf(expectedOutput);
  }

  @Test
  public void emptyWindows() throws Exception {

    // Arrange

    // Act
    testHarness.processWatermark(300L);
    testHarness.processElement(1, 400L);
    testHarness.processWatermark(1000L);
    testHarness.processWatermark(2000L);

    // Assert
    List<StreamRecord<Tuple3<Long, Integer, Integer>>> expectedOutput =
        Arrays.asList(new StreamRecord<>(new Tuple3<>(1000L, 1, 1), 1000L));
    assertThat(testHarness.extractOutputStreamRecords())
        .containsExactlyInAnyOrderElementsOf(expectedOutput);
  }
}
