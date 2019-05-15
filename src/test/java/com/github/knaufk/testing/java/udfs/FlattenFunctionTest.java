package com.github.knaufk.testing.java.udfs;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

public class FlattenFunctionTest {

  private FlattenFunction flattenFunction;

  @Before
  public void setupFlattenFunction() throws Exception {
    flattenFunction = new FlattenFunction();
    flattenFunction.setListStatistics(new DescriptiveStatisticsHistogram(100));
  }

  @Test
  public void listOfIntegersIsFlattened() throws Exception {

    // Arrange
    List<Integer> integers = Arrays.asList(2, 5, 7, 1, 9, 6, 8);
    Collector<Integer> collector = mock(Collector.class);

    // Act
    flattenFunction.flatMap(integers, collector);

    // Assert
    for (Integer integer : integers) {
      verify(collector, times(1)).collect(integer);
    }
  }

  @Test
  public void emptyListIsFlattened() throws Exception {

    // Arrange
    List<Integer> integers = Arrays.asList();
    Collector<Integer> collector = mock(Collector.class);

    // Act
    flattenFunction.flatMap(integers, collector);

    // Assert
    verify(collector, never()).collect(anyInt());
  }

  @Test
  public void histogramIsBuildCorrectly() throws Exception {

    // Arrange
    Collector<Integer> collector = mock(Collector.class);

    // Act
    flattenFunction.flatMap(Arrays.asList(1, 5, 7), collector);
    flattenFunction.flatMap(Arrays.asList(1), collector);
    flattenFunction.flatMap(Arrays.asList(1, 5, 7), collector);
    flattenFunction.flatMap(Arrays.asList(1, 5, 7, 8, 8), collector);

    // Assert
    assertThat(flattenFunction.getListStatistics().getCount()).isEqualTo(4);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMax()).isEqualTo(5);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMin()).isEqualTo(1);
    assertThat(flattenFunction.getListStatistics().getStatistics().getMean())
        .isCloseTo(3.0, within(0.01));
  }
}
