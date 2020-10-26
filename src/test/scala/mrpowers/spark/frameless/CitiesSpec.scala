package mrpowers.spark.frameless
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.TypedDataset
import org.scalatest.FunSpec
import DatasetCreator._
import frameless.functions._
import frameless.functions.nonAggregate._

class CitiesSpec extends FunSpec with DataFrameComparer {

  it("displays a normal dataset") {
    citiesDS.show()
  }

  it("creates a typed dataset") {
    citiesTDS.dataset.show()
  }

  it("selects a column from a regular dataset") {
    val cities = citiesDS.select("population")
    cities.show()
  }

  it("selects a column from a typed dataset") {
    val cities: TypedDataset[Double] = citiesTDS.select(citiesTDS('population))
    cities.dataset.show()
  }

//  it("raises a runtime error if missing column is selected with regular Dataset API") {
//    citiesDS.select("continent")
//  }

  // you can't even run the test suite with this code
  // the code simply does not compile
//  it("raises a compile time error if the column is missing") {
//    citiesTDS.select(DatasetCreator.citiesTDS('continent))
//  }

  it("adds a column to a dataset") {
    case class City2(name: String, population: Double, greeting: String)
    val tds2 = citiesTDS.withColumn[City2](lit("hi"))
    tds2.dataset.show()
  }

  it("allows you to add a column without supplying the entire schema") {
    val tds2 = citiesTDS.withColumnTupled(lit("hi"))
    tds2.dataset.show()
    tds2.dataset.printSchema()
  }

  it("updates a Column") {
    val cities = citiesTDS.select(
      concat(citiesTDS('name), lit(" is fun")),
      citiesTDS('population)
    )
    cities.dataset.show()
//    cities.show().run()
  }

}
