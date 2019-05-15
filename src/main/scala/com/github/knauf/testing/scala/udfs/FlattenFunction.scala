package com.github.knauf.testing.scala.udfs

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class FlattenFunction extends FlatMapFunction[List[Int], Int] {

  override def flatMap(value: List[Int], out: Collector[Int]): Unit = {
    value.foreach(out.collect)
  }
}
