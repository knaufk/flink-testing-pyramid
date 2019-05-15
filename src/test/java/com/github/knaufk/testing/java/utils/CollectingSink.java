package com.github.knaufk.testing.java.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class CollectingSink implements SinkFunction<Tuple3<Long, Integer, Integer>> {

  public static final List<Tuple3<Long, Integer, Integer>> result =
      Collections.synchronizedList(new ArrayList<>());

  public void invoke(Tuple3<Long, Integer, Integer> value, Context context) throws Exception {
    result.add(value);
  }
}
