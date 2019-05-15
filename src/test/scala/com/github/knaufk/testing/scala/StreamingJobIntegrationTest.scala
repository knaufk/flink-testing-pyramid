package com.github.knaufk.testing.scala

import java.util

import com.github.knauf.testing.scala.udfs.FlattenFunction
import org.apache.flink.api.scala._
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class StreamingJobIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(2)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "IncrementFlatMapFunction pipeline" should "incrementValues" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    env.fromElements(List(1, 2, 3), List(1, 2))
       .flatMap(new FlattenFunction())
       .addSink(new CollectSink())

    env.execute("Window Stream WordCount")


    CollectSink.values should contain theSameElementsAs List(1,2,3,1,2)

  }
}

class CollectSink extends SinkFunction[Int] {

  override def invoke(value: Int): Unit = {
    synchronized {
      print("Collecting" + value)
      CollectSink.values.add(value)
    }
  }
}

object CollectSink {
  val values: util.List[Int] = new util.ArrayList()
}




