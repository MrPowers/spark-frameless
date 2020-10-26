package mrpowers.spark.frameless

import frameless.TypedDataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DatasetCreator {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
  implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  import frameless.syntax._

  case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
  val apartments = Seq(
    Apartment("Paris", 50,  300000.0, 2),
    Apartment("Paris", 100, 450000.0, 3),
    Apartment("Paris", 25,  250000.0, 1),
    Apartment("Lyon",  83,  200000.0, 2),
    Apartment("Lyon",  45,  133000.0, 1),
    Apartment("Nice",  74,  325000.0, 3)
  )
  val aptsTDS = TypedDataset.create(apartments)

  case class City(name: String, population: Double)
  val cities = Seq(
    City("Manila", 12.8),
    City("Brasilia", 2.5),
    City("Lagos", 14.4)
  )
  val citiesTDS = TypedDataset.create(cities)
  val citiesDS = spark.createDataset(cities)
}
