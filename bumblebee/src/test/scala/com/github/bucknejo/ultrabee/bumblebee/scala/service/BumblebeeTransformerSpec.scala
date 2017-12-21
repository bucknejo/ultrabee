package com.github.bucknejo.ultrabee.bumblebee.scala.service

import com.github.bucknejo.ultrabee.bumblebee.scala.util.BumblebeeUtility
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class BumblebeeTransformerSpec extends FlatSpec with Matchers with BumblebeeUtility {

  val extractor = new BumblebeeExtractor
  val transformer = new BumblebeeTransformer

  val appName = "transformer-spec"
  val master = "local[*]"
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  "joinManufacturersWithProducts" should "return a DataFrame of joined data between manufacturers and products" in {

    import spark.implicits._

    val manufacturers = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS)
    val products = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_PRODUCTS)
    val joined = transformer.joinManufacturersWithProducts(spark, manufacturers, products)
    val filtered = joined.filter($"manufacturer_id" === "RRTL-MT6227-0318").orderBy("product_id")

    assert(filtered.count() == 16)

  }

}
