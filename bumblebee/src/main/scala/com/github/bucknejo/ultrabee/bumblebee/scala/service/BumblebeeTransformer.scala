package com.github.bucknejo.ultrabee.bumblebee.scala.service

import org.apache.spark.sql.{DataFrame, SparkSession}

class BumblebeeTransformer {

  def joinManufacturersWithProducts(spark: SparkSession, manufacturers: DataFrame, products: DataFrame): DataFrame = {

    import spark.implicits._

    manufacturers
      .alias("m")
      .join(products.alias("p"), $"m.manufacturer_id" === $"p.manufacturer_id", joinType = "inner")
      .select(
        $"m.manufacturer_id",
        $"m.name".as("manufacturer_name"),
        $"p.product_id",
        $"p.name".as("product_name"),
        $"p.is_available",
        $"p.is_discontinued",
        $"p.is_manufactured"
      )

  }

  def locations(spark: SparkSession, manufacturers: DataFrame): DataFrame = {
    import spark.implicits._
    manufacturers.select($"location.address1", $"location.city", $"location.state", $"location.zip", $"location.country")
  }

}
