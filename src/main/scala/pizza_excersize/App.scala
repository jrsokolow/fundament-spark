package pizza_excersize

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local").getOrCreate()

    import org.apache.spark.sql.functions._

    val pizzaDataDataFrame = spark.read.option("header", "true").csv("src/main/scala/test_data/pizza_data.csv")
    pizzaDataDataFrame.printSchema()

    val newPizzaDataDataFrame = pizzaDataDataFrame.withColumn("Price", regexp_replace(col
    ("Price"), "[$,]", "").cast(DoubleType))
    newPizzaDataDataFrame.show(1000000)

    val minPrice = newPizzaDataDataFrame.agg(min("Price")).first().getDouble(0)
    println("min price: " + minPrice)

    val maxPrice = newPizzaDataDataFrame.agg(max("Price")).first().getDouble(0)
    println("max price: " + maxPrice)

    val avgPrice = newPizzaDataDataFrame.agg(avg("Price")).first().getDouble(0)
    println("avg price: " + avgPrice)
  }

}
