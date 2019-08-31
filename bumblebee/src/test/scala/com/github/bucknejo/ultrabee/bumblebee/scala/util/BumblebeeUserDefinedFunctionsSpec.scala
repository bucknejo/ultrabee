package com.github.bucknejo.ultrabee.bumblebee.scala.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class BumblebeeUserDefinedFunctionsSpec extends FlatSpec with Matchers with BumblebeeUserDefinedFunctions with BumblebeeUtility {

  import spark.implicits._

  "udf updateTags" should "return a column with updated tags" in {

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
