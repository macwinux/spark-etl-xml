package transformers

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class TransformersSpec extends AnyFlatSpec {
  lazy val spark: SparkSession = SparkSession.builder.config("spark.master", "local").getOrCreate()
  val xmlFile = spark.read.format("com.databricks.spark.xml")
    .option("rootTag", "catalog")
    .option("rowTag", "product")
    .load("src/main/resources/catalog.xml")

  "A example" should "read the file" in {
    xmlFile.printSchema()
    assert(xmlFile.count() === 1)
  }


  "Transform a first level value" should "be correct" in {

    val schema =  StructType(
      List(
        StructField("_description", StringType),
        StructField("columnReplace", StringType)
      )
    )
    val dict = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("Cardigan Sweater","Cardigan Replace"))), schema)
    xmlFile.show(2)
    val transform = Transformers.firstLevelValue(xmlFile, dict, "_description", "columnReplace").drop("columnReplace")
    transform.show(2)
    assert(transform.where("_product_image = 'cardigan.jpg'").collect().head.getAs("_description").equals("Cardigan Replace"))
  }

  "Transform a second level value" should "be correct" in {
    val schema =  StructType(
      List(
        StructField("item_number", StringType),
        StructField("columnReplace", StringType)
      )
    )
    xmlFile.show(10)
    val dict = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("QWZ5671","QWZ5673"))), schema)
    val transform = Transformers
      .secondLevelValue(xmlFile,dict,"catalog_item","item_number","columnReplace")
    transform.show(10)
  }
}
