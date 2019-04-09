package com.github.knaufk.udfs;

import java.util.List;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.util.Collector;

/**
 * A stateless {@link FlatMapFunction}, which takes a {@link List} of {@link Integer}s and returns
 * each element from the list as a separate stream record.
 */
public class FlattenFunction extends RichFlatMapFunction<List<Integer>, Integer> {

  private DescriptiveStatisticsHistogram listStatistics;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    listStatistics =
        getRuntimeContext()
            .getMetricGroup()
            .histogram("listStatistics", new DescriptiveStatisticsHistogram(1000));
  }

  @Override
  public void flatMap(List<Integer> integers, Collector<Integer> collector) throws Exception {
    for (Integer integer : integers) {
      collector.collect(integer);
    }
    listStatistics.update(integers.size());
  }

  @VisibleForTesting
  DescriptiveStatisticsHistogram getListStatistics() {
    return listStatistics;
  }

  @VisibleForTesting
  void setListStatistics(DescriptiveStatisticsHistogram listStatistics) {
    this.listStatistics = listStatistics;
  }
}
