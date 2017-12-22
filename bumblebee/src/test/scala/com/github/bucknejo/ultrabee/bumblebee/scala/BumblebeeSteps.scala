package com.github.bucknejo.ultrabee.bumblebee.scala

import java.io.File

import com.github.bucknejo.ultrabee.bumblebee.scala.service.{BumblebeeExtractor, BumblebeeLoader, BumblebeeTransformer}
import com.github.bucknejo.ultrabee.bumblebee.scala.util.BumblebeeUtility
import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

class BumblebeeSteps extends ScalaDsl with EN with BumblebeeUtility {

  val appName = "bumblebee-steps"
  val master = "local[*]"
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  val results: mutable.Builder[(String, Map[String, DataFrame]), Map[String, Map[String, DataFrame]]] =
    Map.newBuilder[String, Map[String, DataFrame]]

  val EXTRACT = "EXTRACT"
  val TRANSFORM = "TRANSFORM"
  val LOAD = "LOAD"

  Before { scenario =>
    logger.info(s"s[${this.getClass.getName}] Before scenario: ${scenario.getName}")
    spark.newSession()
    results.clear()
  }

  After { scenario =>
    logger.info(s"s[${this.getClass.getName}] After scenario: ${scenario.getName}")
  }

  Given("""^Set environment variables$""") { () =>

  }

  When("""^Run Bumblebee for ?(.*)$""") { (date: String) =>
    logger.info(s"s[${this.getClass.getName}] date: $date")

    val extractor = new BumblebeeExtractor
    val transformer = new BumblebeeTransformer
    val loader = new BumblebeeLoader
    val bumblebee = new Bumblebee(extractor, transformer, loader)

    val extract: Map[String, DataFrame] = bumblebee.extract(spark, Array())
    val transform: Map[String, DataFrame] = bumblebee.transform(spark, Array(), extract)
    val load: Map[String, DataFrame] = bumblebee.load(spark, Array(), transform)

    results.+=(EXTRACT -> extract)
    results.+=(TRANSFORM -> transform)
    results.+=(LOAD -> load)

  }

  When("""^Filter ?(.*) from ?(.*) order by ?(.*)$""") { (name: String, level: String, orderBy: String, data: DataTable) =>

    import spark.implicits._

    val df: DataFrame = results.result()(level)(name)
    val filterColumns = convertDataTableToDataFrame(data)

    val filters = Map.newBuilder[String, List[String]]
    val orders = orderBy.split(',').toList

    filterColumns.columns.foreach(name => {
      filters.+=(name -> filterColumns.select(name).map(r => r(0).asInstanceOf[String]).collect().toList.distinct)
    })

    val filtered = filterDataFrame(df, filters.result())
    val ordered = orderDataFrame(filtered, orders)

    results.+=(TRANSFORM -> Map("MANUFACTURERS_FILTER_CITY_STATE" -> ordered))

  }

  Then("""^Compare DataFrame ?(.*) from ?(.*) order by ?(.*)$""") { (name: String, level: String, orderBy: String, data: DataTable) =>

    logger.info(s"s[${this.getClass.getName}] name: $name")
    logger.info(s"s[${this.getClass.getName}] level: $level")
    logger.info(s"s[${this.getClass.getName}] orderBy: $orderBy")

    val df: DataFrame = results.result()(level)(name)
    val orders = orderBy.split(',').toList
    val header = headersFromRawDataTable(data)
    val take = rowsFromRawDataTable(data).size
    val actual = orderDataFrame(df.select(columnsFromHeader(header).map(col): _*), orders).take(take)
    val expected = convertDataTableToDataFrame(data).collect()


    assert(actual.length == expected.length)

    for (i <- actual.indices) {
      val a = actual(i).mkString(",")
      val e = expected(i).mkString(",")
      assert(a == e)
    }

  }

  def autoClose[A <: AutoCloseable, B](resource: A)(code: A => B): Try[B] = {
    val tryResult = Try {
      code(resource)
    }
    resource.close()
    tryResult
  }

  def columnsFromHeader(header: List[String]): List[String] = {
    header.map(_.split(':')(0))
  }

  def convertDataTableToDataFrame(data: DataTable): DataFrame = {
    val fieldSpec = data
      .topCells().asScala
      .map(_.split(':'))
      .map(splits => (splits(0), splits(1).toLowerCase))
      .map {
        case (name, "string") => (name, DataTypes.StringType)
        case (name, "double") => (name, DataTypes.DoubleType)
        case (name, "int") => (name, DataTypes.IntegerType)
        case (name, "integer") => (name, DataTypes.IntegerType)
        case (name, "long") => (name, DataTypes.LongType)
        case (name, "boolean") => (name, DataTypes.BooleanType)
        case (name, "bool") => (name, DataTypes.BooleanType)
        case (name, _) => (name, DataTypes.StringType)
      }

    val schema = StructType(
      fieldSpec
        .map { case (name, dataType) =>
            StructField(name, dataType, nullable = false)
        }
    )

    val rows = data
      .asMaps(classOf[String], classOf[String]).asScala
      .map { row =>
        val values = row
          .values().asScala
          .zip(fieldSpec)
          .map { case (v, (fn, dt)) => (v, dt) }
          .map {
            case (v, DataTypes.IntegerType) => v.toInt
            case (v, DataTypes.DoubleType) => v.toDouble
            case (v, DataTypes.LongType) => v.toLong
            case (v, DataTypes.BooleanType) => v.toBoolean
            case (v, DataTypes.StringType) => v
          }
          .toSeq

        Row.fromSeq(values)

      }
      .toList

    val df = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df
  }

  def dataTableAsMap(dataTable: DataTable): Map[String, String] = {
    dataTable.asMap(classOf[String], classOf[String]).asScala.tail.toMap
  }

  def fileContentsToString(file: File): String = {
    val source = Source.fromFile(file)
    try source.mkString finally source.close()
  }

  def headersFromRawDataTable(dataTable: DataTable): List[String] = {
    dataTable.raw().get(0).asScala.toList
  }

  def rowsFromRawDataTable(dataTable: DataTable): List[List[String]] = {
    val lists = List.newBuilder[List[String]]
    dataTable.raw().size() match {
      case size if size > 0 => dataTable.raw().subList(1, dataTable.raw().size()).asScala.toList.map(_.asScala.toList)
      case _ => lists.result()
    }
  }

}
