package com.github.knaufk.testing.scala.udfs

import com.github.knauf.testing.scala.udfs.FlattenFunction
import org.apache.flink.util.Collector
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec


class FlattenFunctionTest extends FlatSpec with MockFactory {

    "FlattenFunction" should "flatten list of integers" in {

      val flattenFunction : FlattenFunction = new FlattenFunction()

      // Arrange
      val integers: List[Int] = List(2, 5, 7, 1, 9, 6, 8)
      val collector =  mock[Collector[Int]]

      // Assert
      integers.foreach(integer =>
        (collector.collect _).expects(integer)
      )

      // Act
      flattenFunction.flatMap(integers, collector)
  }

}
