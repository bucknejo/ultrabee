package com.github.bucknejo.ultrabee.bumblebee.scala.util

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class BumblebeeUtilitySpec extends FlatSpec with Matchers with BumblebeeUtility {

  "filterDataFrame" should "recursively filter a DataFrame" in {

    val result = Seq(
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("B", "F", "D")
    )

    val rows = Seq(
      TestUtilityDataFrame("A", "B", "C"),
      TestUtilityDataFrame("A", "D", "D"),
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "D", "D"),
      TestUtilityDataFrame("B", "E", "D"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "D", "D"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "F", "D")
    )

    val filters = Map.newBuilder[String, List[String]]
    filters.+=("f1" -> List("A", "B"))
    filters.+=("f2" -> List("F"))

    val df = spark.createDataFrame(rows)

    val actual = filterDataFrame(df, filters.result())
    val expected = spark.createDataFrame(result)

    actual.printSchema()
    actual.show()

    assert(actual.except(expected).count() === 0)

  }

  it should "dismiss filter columns not found in provided DataFrame" in {

    val result = Seq(
      TestUtilityDataFrame("A", "B", "C"),
      TestUtilityDataFrame("A", "D", "D"),
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D")
    )

    val rows = Seq(
      TestUtilityDataFrame("A", "B", "C"),
      TestUtilityDataFrame("A", "D", "D"),
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "D", "D"),
      TestUtilityDataFrame("B", "E", "D"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "D", "D"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "F", "D")
    )

    val filters = Map.newBuilder[String, List[String]]
    filters.+=("f1" -> List("A"))
    filters.+=("f4" -> List("F"))

    val df = spark.createDataFrame(rows)

    val actual = filterDataFrame(df, filters.result())
    val expected = spark.createDataFrame(result)

    actual.printSchema()
    actual.show()

    assert(actual.except(expected).count() === 0)

  }

  "orderDataFrame" should "recursively order a DataFrame" in {

    val result = Seq(
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "X", "D"),
      TestUtilityDataFrame("A", "Z", "C"),
      TestUtilityDataFrame("B", "A", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("B", "Y", "D"),
      TestUtilityDataFrame("B", "Z", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "X", "D"),
      TestUtilityDataFrame("C", "Z", "D")
    )


    val rows = Seq(
      TestUtilityDataFrame("A", "Z", "C"),
      TestUtilityDataFrame("A", "X", "D"),
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "Z", "D"),
      TestUtilityDataFrame("C", "X", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "Z", "D"),
      TestUtilityDataFrame("B", "Y", "D"),
      TestUtilityDataFrame("B", "A", "D"),
      TestUtilityDataFrame("B", "F", "D")
    )

    val orders = List("f2", "f1")

    val df = spark.createDataFrame(rows)

    val actual = orderDataFrame(df, orders)
    val expected = spark.createDataFrame(result)

    actual.printSchema()
    actual.show()

    assert(actual.except(expected).count() === 0)

  }

  it should "dismiss order columns not found in provided DataFrame" in {

    val result = Seq(
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "X", "D"),
      TestUtilityDataFrame("A", "Z", "C"),
      TestUtilityDataFrame("B", "A", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "F", "D"),
      TestUtilityDataFrame("B", "Y", "D"),
      TestUtilityDataFrame("B", "Z", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "X", "D"),
      TestUtilityDataFrame("C", "Z", "D")
    )


    val rows = Seq(
      TestUtilityDataFrame("A", "Z", "C"),
      TestUtilityDataFrame("A", "X", "D"),
      TestUtilityDataFrame("A", "E", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("A", "F", "D"),
      TestUtilityDataFrame("C", "B", "C"),
      TestUtilityDataFrame("C", "Z", "D"),
      TestUtilityDataFrame("C", "X", "D"),
      TestUtilityDataFrame("C", "F", "D"),
      TestUtilityDataFrame("C", "E", "D"),
      TestUtilityDataFrame("B", "B", "C"),
      TestUtilityDataFrame("B", "Z", "D"),
      TestUtilityDataFrame("B", "Y", "D"),
      TestUtilityDataFrame("B", "A", "D"),
      TestUtilityDataFrame("B", "F", "D")
    )

    val orders = List("f4", "f2", "f1")

    val df = spark.createDataFrame(rows)

    val actual = orderDataFrame(df, orders)
    val expected = spark.createDataFrame(result)

    actual.printSchema()
    actual.show()

    assert(actual.except(expected).count() === 0)

  }

  case class TestUtilityDataFrame(f1: String, f2: String, f3: String)
  case class Filter(name: String, filter: List[String])

}
