package com.github.bucknejo.ultrabee.bumblebee.scala.util

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class BumblebeeUserDefinedFunctionsSpec extends FlatSpec with Matchers with BumblebeeUserDefinedFunctions {

  val appName = "udf-spec"
  val master = "local[*]"
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  "udf updateTags" should "return a column with updated tags" in {

    import spark.implicits._

    val original = Array("TEST01", "TEST02", "TEST03")
    val updates = Array("UPDATE01", "TEST02", "UPDATE03")

    val tags = Seq(
      Tags(original)
    )

    val df = spark.createDataFrame(tags)
    val res = df.select($"tags", updateTags(updates)($"tags").as("newTags"))
    val actual = res.select($"newTags").take(1).last.getList[String](0)

    assert(actual.toArray sameElements  (original ++ updates).distinct)

  }

  it should "return original array when updates are null" in {

    import spark.implicits._

    val original = Array("TEST01", "TEST02", "TEST03")
    val updates = null

    val tags = Seq(
      Tags(original)
    )

    val df = spark.createDataFrame(tags)
    val res = df.select($"tags", updateTags(updates)($"tags").as("newTags"))
    val actual = res.select($"newTags").take(1).last.getList[String](0)

    assert(actual.toArray sameElements  original)

  }

  it should "return updates array when originals are null" in {

    import spark.implicits._

    val original = null
    val updates = Array("TEST01", "TEST02", "TEST03")

    val tags = Seq(
      Tags(original)
    )

    val df = spark.createDataFrame(tags)
    val res = df.select($"tags", updateTags(updates)($"tags").as("newTags"))
    val actual = res.select($"newTags").take(1).last.getList[String](0)

    assert(actual.toArray sameElements  updates)

  }

  case class Tags(tags: Array[String])

}
