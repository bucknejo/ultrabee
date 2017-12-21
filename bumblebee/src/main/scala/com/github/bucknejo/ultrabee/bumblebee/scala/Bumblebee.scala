package com.github.bucknejo.ultrabee.bumblebee.scala

import com.github.bucknejo.ultrabee.api.scala.Ingestible
import com.github.bucknejo.ultrabee.bumblebee.scala.service.{BumblebeeExtractor, BumblebeeLoader, BumblebeeTransformer}
import com.github.bucknejo.ultrabee.bumblebee.scala.util.{BumblebeeConstants, BumblebeeException, BumblebeeUtility}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Bumblebee(extractor: BumblebeeExtractor, transformer: BumblebeeTransformer, loader: BumblebeeLoader) extends Ingestible with BumblebeeUtility {

  override def extract(spark: SparkSession, args: Array[String]): Map[String, DataFrame] = {
    val dataFrameMap = Map.newBuilder[String, DataFrame]

    try {

      dataFrameMap.+=(BumblebeeConstants.MANUFACTURERS -> extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS))
      dataFrameMap.+=(BumblebeeConstants.PRODUCTS -> extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_PRODUCTS))

    } catch {
      case bumblebee: BumblebeeException =>
        logBumblebeeException(bumblebee, this.getClass.getName)
      case e: Exception =>
        logger.error(s"[${this.getClass.getName}] [EXTRACT] ${BumblebeeConstants.BANNER_ERROR}")
        logger.error(s"[${this.getClass.getName}] [EXTRACT]", e)
        logger.error(s"[${this.getClass.getName}] [EXTRACT] ${BumblebeeConstants.BANNER_ERROR}")
    }

    dataFrameMap.result()

  }

  // TODO: do something to transform provided DataFrame(s)
  override def transform(spark: SparkSession, args: Array[String], extract: Map[String, DataFrame]): Map[String, DataFrame] = {
    val dataFrameMap = Map.newBuilder[String, DataFrame]

    try {

    } catch {
      case bumblebee: BumblebeeException =>
        logBumblebeeException(bumblebee, this.getClass.getName)
      case e: Exception =>
        logger.error(s"[${this.getClass.getName}] [TRANSFORM] ${BumblebeeConstants.BANNER_ERROR}")
        logger.error(s"[${this.getClass.getName}] [TRANSFORM]", e)
        logger.error(s"[${this.getClass.getName}] [TRANSFORM] ${BumblebeeConstants.BANNER_ERROR}")
    }

    dataFrameMap.result()
  }

  // TODO: do something to load/write provided DataFrame(s)
  override def load(spark: SparkSession, args: Array[String], transform: Map[String, DataFrame]): Map[String, DataFrame] = {
    val dataFrameMap = Map.newBuilder[String, DataFrame]

    try {

    } catch {
      case bumblebee: BumblebeeException =>
        logBumblebeeException(bumblebee, this.getClass.getName)
      case e: Exception =>
        logger.error(s"[${this.getClass.getName}] [LOAD] ${BumblebeeConstants.BANNER_ERROR}")
        logger.error(s"[${this.getClass.getName}] [LOAD]", e)
        logger.error(s"[${this.getClass.getName}] [LOAD] ${BumblebeeConstants.BANNER_ERROR}")
    }

    dataFrameMap.result()
  }

}

