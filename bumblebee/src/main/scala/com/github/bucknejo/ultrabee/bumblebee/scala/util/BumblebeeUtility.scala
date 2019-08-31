package com.github.bucknejo.ultrabee.bumblebee.scala.util

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait BumblebeeUtility {

  @transient lazy val logger: Logger = LogManager.getLogger("Bumblebee")

  val appName = "spark-local-tester"
  val master = "local"
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  val properties: Properties = Try({
    val prop: Properties = new Properties()
    val path = "bumblebee.properties"
    prop.load(new FileInputStream(new File(getClass.getClassLoader.getResource(path).getFile)))
    prop
  }) match {
    case Success(s) => s
    case Failure(f) =>
      logger.error(s"[${this.getClass.getName}] Could not load properties for Bumblebee", f)
      throw f
  }

  val STATIC_DATA_PATH_LOCAL: Boolean = properties.getProperty("static.data.path.local").toBoolean
  val STATIC_DATA_PATH_MANUFACTURERS: String =
    staticDataPath(properties.getProperty("static.data.path.manufacturers"), STATIC_DATA_PATH_LOCAL)
  val STATIC_DATA_PATH_PRODUCTS: String =
    staticDataPath(properties.getProperty("static.data.path.products"), STATIC_DATA_PATH_LOCAL)

  def logBumblebeeException(bumblebee: BumblebeeException, className: String): Unit = {
    logger.error(s"[$className] ${BumblebeeConstants.BANNER_ERROR}")
    logger.error(s"[$className] Level: ${bumblebee.returnCode.level}")
    logger.error(s"[$className] Code: ${bumblebee.returnCode.code}")
    logger.error(s"[$className] Description: ${bumblebee.returnCode.description}")
    logger.error(s"[$className] Link: ${bumblebee.returnCode.link}")
    logger.error(s"[$className] ${BumblebeeConstants.BANNER_ERROR}")
  }

  def staticDataPath(path: String, isLocal: Boolean): String = {
    if (isLocal)
      new File(getClass.getClassLoader.getResource(path).getFile).getAbsolutePath
    else
      path
  }

  @tailrec final def filterDataFrame(dataFrame: DataFrame, filters: Map[String, List[String]]): DataFrame = {

    import org.apache.spark.sql.functions.col

    if (filters.isEmpty) dataFrame
    else {
      val filter = filters.head
      val reducedFilters = filters.filterKeys(_ != filter._1)
      val filteredDataFrame = if (dataFrame.columns.contains(filter._1)) {
        dataFrame.filter(col(filter._1).isin(filter._2: _*))
      } else {
        dataFrame
      }
      filterDataFrame(filteredDataFrame, reducedFilters)
    }

  }

  @tailrec final def orderDataFrame(dataFrame: DataFrame, orders: List[String]): DataFrame = {

    if (orders.isEmpty) dataFrame
    else {
      val order = orders.head
      val reducedOrders = orders.tail
      val orderedDataFrame = if (dataFrame.columns.contains(order)) {
        dataFrame.orderBy(order)
      } else {
        dataFrame
      }
      orderDataFrame(orderedDataFrame, reducedOrders)
    }

  }

}
