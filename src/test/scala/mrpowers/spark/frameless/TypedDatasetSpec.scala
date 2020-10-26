package mrpowers.spark.frameless
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.TypedDataset
import org.scalatest.FunSpec

class TypedDatasetSpec extends FunSpec with DataFrameComparer {

  it("creates a typed dataset") {
    DatasetCreator.aptsTDS.dataset.show()
  }

  it("selects a column from a typed dataset") {
    val cities: TypedDataset[String] = DatasetCreator.aptsTDS.select(DatasetCreator.aptsTDS('city))
    cities.dataset.show()
  }

  // you can't even run the test suite with this code
  // the code simply does not compile
//  it("raises a compile time error if the column is misspelled") {
//    DatasetCreator.aptsTDS.select(DatasetCreator.aptsTDS('citi))
//  }

}
