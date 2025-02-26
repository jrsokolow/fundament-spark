package twitter

import org.apache.spark.sql.SparkSession

object TwitterApp {
  def main(args: Array[String]): Unit = {
    def spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local[*]")
      .getOrCreate()

    val grammysDF = spark.read.option("header", "true").csv("src/main/scala/test_data/GRAMMYs_tweets.csv")
    grammysDF.show(false)
    grammysDF.printSchema()

    val financialDF = spark.read.option("header", "true").csv("src/main/scala/test_data/financial.csv")
    financialDF.show(false)
    financialDF.printSchema()

    val covidTweetsDF = spark.read.option("header", "true").csv("src/main/scala/test_data/covid19_tweets.csv")
    covidTweetsDF.show(false)
    covidTweetsDF.printSchema()
  }
}
