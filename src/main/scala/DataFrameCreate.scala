import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
object DataFrameCreate  extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Application 1")
    .master("local[*]")
    .getOrCreate()

  val ordersDf = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/orders.csv")

  ordersDf.show(2)
  ordersDf.printSchema()
}
