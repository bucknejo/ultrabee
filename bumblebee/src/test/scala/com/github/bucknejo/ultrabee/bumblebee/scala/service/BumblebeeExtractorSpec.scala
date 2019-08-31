package com.github.bucknejo.ultrabee.bumblebee.scala.service

import com.github.bucknejo.ultrabee.bumblebee.scala.util.BumblebeeUtility
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class BumblebeeExtractorSpec extends FlatSpec with Matchers with BumblebeeUtility {

  val extractor = new BumblebeeExtractor

  import spark.implicits._

  "loadDataFrameFromJson" should "load a manufacturers JSON file into a dataframe" in {

    val df = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS)
    df.printSchema()
    df.show()
    assert(df.count() === 300)

  }

  it should "return 20 manufacturer ids" in {

    val df = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS)
    val actual = df.select($"manufacturer_id").limit(20)

    actual.collect().foreach(println)
    actual.cache()

    assert(actual.count() === 20)

  }

  it should "load a products JSON file into a dataframe" in {

    val df = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_PRODUCTS)
    df.printSchema()
    df.show()
    assert(df.count() === 300)

  }

}
