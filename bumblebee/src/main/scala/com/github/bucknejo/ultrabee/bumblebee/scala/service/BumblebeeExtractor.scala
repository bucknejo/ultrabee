package com.github.bucknejo.ultrabee.bumblebee.scala.service

import com.github.bucknejo.ultrabee.bumblebee.scala.util.BumblebeeUtility
import org.apache.spark.sql.{DataFrame, SparkSession}

class BumblebeeExtractor extends BumblebeeUtility {

  def loadDataFrameFromJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("multiLine", "true").json(path)
  }

}
