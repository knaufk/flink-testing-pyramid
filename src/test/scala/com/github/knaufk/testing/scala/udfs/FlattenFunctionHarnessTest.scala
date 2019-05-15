package com.github.knaufk.testing.scala.udfs

import com.github.knauf.testing.scala.udfs.FlattenFunction
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.streaming.util.{OneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import java.util

class FlattenFunctionHarnessTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val ANY_TIMESTAMP: Long = 0

  private var testHarness: OneInputStreamOperatorTestHarness[List[Int], Int] = null
  private var flattenFunction: FlattenFunction = null

  before {
    flattenFunction = new FlattenFunction
    testHarness = new OneInputStreamOperatorTestHarness(new StreamFlatMap[List[Int], Int](flattenFunction))
    testHarness.open()
  }

    "FlattenFunction" should "flatten list of integers" in {

      // Arrange
      val integers: List[Int] = List(2, 5, 7, 1, 9, 6, 8)

      // Act
      testHarness.processElement(integers, ANY_TIMESTAMP)

      // Assert
      val output : util.List[Int] = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput)
      output should contain theSameElementsAs List(2, 5, 7, 1, 9, 6, 8)
    }
}
