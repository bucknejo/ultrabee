package com.github.bucknejo.ultrabee.api.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Ingestible {

  def extract(spark: SparkSession, args: Array[String]): Map[String, DataFrame]
  def transform(spark: SparkSession, args: Array[String], extract: Map[String, DataFrame]): Map[String, DataFrame]
  def load(spark: SparkSession, args: Array[String], transform: Map[String, DataFrame]): Map[String, DataFrame]

}
