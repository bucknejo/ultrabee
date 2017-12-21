package com.github.bucknejo.ultrabee.core.scala

import org.apache.spark.sql.SparkSession

object Ingestion {

  def main(args: Array[String]): Unit = {
    run(args)
  }

  def run(args: Array[String]): Int = {

    val appName = "ultrabee"
    val master = "local[*]"
    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    args.foreach(arg => {
      IngestionFactory(arg) match {
        case Some(i) =>
          val extract = i.extract(spark, args)
          val transform = i.transform(spark, args, extract)
          i.load(spark, args, transform)
        case None =>
      }
    })

    0

  }

}
