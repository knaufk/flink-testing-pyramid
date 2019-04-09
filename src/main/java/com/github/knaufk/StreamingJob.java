package com.github.knaufk;

import com.github.knaufk.udfs.BoundedOutOfOrdernessWatermarkAssigner;
import com.github.knaufk.udfs.EvenTimeWindowCounter;
import com.github.knaufk.udfs.FlattenFunction;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A simple streaming job, which takes {@link List<Integer>}s and returns the per-second count for
 * each of the elements contained within those lists.
 */
public class StreamingJob {

  private SourceFunction<List<Integer>> source;
  private SinkFunction<Tuple3<Long, Integer, Integer>> sink;

  public StreamingJob(
      SourceFunction<List<Integer>> source, SinkFunction<Tuple3<Long, Integer, Integer>> sink) {
    this.source = source;
    this.sink = sink;
  }

  public void execute() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    SingleOutputStreamOperator<List<Integer>> listSingleOutputStreamOperator =
        env.addSource(source)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessWatermarkAssigner<>(Time.of(100, TimeUnit.MILLISECONDS)));
    listSingleOutputStreamOperator
        .flatMap(new FlattenFunction())
        .keyBy(integer -> integer)
        .process(new EvenTimeWindowCounter(Time.of(1, TimeUnit.SECONDS)))
        .addSink(sink);

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    StreamingJob job = new StreamingJob(new IntegerListSource(), new PrintSinkFunction<>());
    job.execute();
  }
}
