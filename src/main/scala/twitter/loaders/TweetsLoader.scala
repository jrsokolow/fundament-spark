package twitter.loaders

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import twitter.loaders.TweetsLoader.{COVID_LABEL, FINANCIAL_LABEL, GRAMMYS_LABEL}

object TweetsLoader {
  val COVID_LABEL = "covid"
  val FINANCIAL_LABEL = "financial"
  val GRAMMYS_LABEL = "grammys"
}

class TweetsLoader(sparkSession: SparkSession) {

  def loadALlTweets(): Dataset[Row] = {
    val covidDF: Dataset[Row] = loadCovid();
    val financialDF: Dataset[Row] = loadFinancial();
    val grammyDF: Dataset[Row] = loadGrammy();

    covidDF.unionByName(financialDF, allowMissingColumns = true).unionByName(grammyDF, allowMissingColumns = true)
  }

  private def loadCovid(): Dataset[Row] = {
    sparkSession.read.option("header", "true").csv("src/main/scala/test_data/GRAMMYs_tweets.csv")
      .withColumn("category", lit(COVID_LABEL))
      .na.drop()
  }

  private def loadFinancial(): Dataset[Row] = {
    sparkSession.read.option("header", "true").csv("src/main/scala/test_data/financial.csv")
      .withColumn("category", lit(FINANCIAL_LABEL))
      .na.drop()
  }

  private def loadGrammy(): Dataset[Row] = {
    sparkSession.read.option("header", "true").csv("src/main/scala/test_data/covid19_tweets.csv")
      .withColumn("category", lit(GRAMMYS_LABEL))
      .na.drop()
  }
}
