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


  it should "explode a location column" in {

    import spark.implicits._

    val df = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS)

    val locations = df.select($"location.address1", $"location.city", $"location.state", $"location.zip", $"location.country")

    val ordered = locations.orderBy($"city").orderBy($"state")
    ordered.show(50)

    val california = locations.filter($"state" === "California" and $"city" === "San Diego").orderBy($"zip")
    val arizona = locations.filter($"state" === "Arizona" and $"city" === "Tucson").orderBy($"zip")

    california.show()
    arizona.show()

    assert(california.count() == 4)
    assert(arizona.count() == 5)

  }

  it should "create lists for each column from DataFrame" in {

    import spark.implicits._

    val df = extractor.loadDataFrameFromJson(spark, STATIC_DATA_PATH_MANUFACTURERS)

    val locations = df.select($"location.address1", $"location.city", $"location.state", $"location.zip", $"location.country")

    val rows = Seq(
      CityStateFilter("Escondido", "California"),
      CityStateFilter("San Diego", "California"),
      CityStateFilter("San Jose", "California"),
      CityStateFilter("Phoenix", "Arizona"),
      CityStateFilter("Tucson", "Arizona")
    )

    val filterColumns = spark.createDataFrame(rows)

    val filters = Map.newBuilder[String, List[String]]

    filterColumns.columns.foreach(name => {
      filters.+=(name -> filterColumns.select(name).map(r => r(0).asInstanceOf[String]).collect().toList.distinct)
    })

    val actual = filterDataFrame(locations, filters.result()).orderBy($"zip")

    assert(actual.count() == 15)

  }

  case class CityStateFilter(city: String, state: String)


}
