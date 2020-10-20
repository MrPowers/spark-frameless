package mrpowers.spark.frameless

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  implicit val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
