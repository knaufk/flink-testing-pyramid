package com.github.knaufk.udfs;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

public class FlattenFunctionHarnessTest {

  private static final long ANY_TIMESTAMP = 0;

  private OneInputStreamOperatorTestHarness<List<Integer>, Integer> testHarness;
  private FlattenFunction flattenFunction;

  @Before
  public void setupTestHarness() throws Exception {
    flattenFunction = new FlattenFunction();
    testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(flattenFunction));
    testHarness.open();
  }

  @Test
  public void listOfIntegersIsFlattened() throws Exception {

    // Arrange
    List<Integer> integers = Arrays.asList(2, 5, 7, 1, 9, 6, 8);

    // Act
    testHarness.processElement(integers, ANY_TIMESTAMP);

    // Assert
    assertThat(testHarness.getOutput()).containsExactlyInAnyOrderElementsOf(wrap(integers));
  }

  @Test
  public void emptyListIsFlattened() throws Exception {

    // Arrange

    // Act
    testHarness.processElement(Arrays.asList(), ANY_TIMESTAMP);

    // Assert
    assertThat(testHarness.getOutput()).isEmpty();
  }

  @Test
  public void histogramIsBuildCorrectly() throws Exception {

    // Arrange

    // Act
    testHarness.processElement(Arrays.asList(1, 5, 7), ANY_TIMESTAMP);
    testHarness.processElement(Arrays.asList(1), ANY_TIMESTAMP);
    testHarness.processElement(Arrays.asList(1, 5, 7), ANY_TIMESTAMP);
    testHarness.processElement(Arrays.asList(1, 5, 7, 8, 8), ANY_TIMESTAMP);

    // Assert
    assertThat(flattenFunction.getListStatistics().getCount()).isEqualTo(4);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMax()).isEqualTo(5);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMin()).isEqualTo(1);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMean())
        .isCloseTo(3.0, within(0.01));
  }

  public static <T> List<StreamRecord<T>> wrap(List<T> elements) {
    return elements
        .stream()
        .map(element -> new StreamRecord<>(element, ANY_TIMESTAMP))
        .collect(Collectors.toList());
  }
}
