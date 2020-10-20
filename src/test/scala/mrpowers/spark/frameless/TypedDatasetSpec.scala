package mrpowers.spark.frameless
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.TypedDataset
import org.scalatest.FunSpec

class TypedDatasetSpec
  extends FunSpec
  with DataFrameComparer {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
  implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  import frameless.syntax._

  it("creates a typed dataset") {

    case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
    val apartments = Seq(
      Apartment("Paris", 50,  300000.0, 2),
      Apartment("Paris", 100, 450000.0, 3),
      Apartment("Paris", 25,  250000.0, 1),
      Apartment("Lyon",  83,  200000.0, 2),
      Apartment("Lyon",  45,  133000.0, 1),
      Apartment("Nice",  74,  325000.0, 3)
    )
    val aptTypedDS = TypedDataset.create(apartments)
    aptTypedDS.show()
//    import frameless.syntax._
//    val aptDs = spark.createDataset(apartments)
//    val aptTypedDs2 = aptDs.typed
//    aptTypedDs2.show()
  }

}
