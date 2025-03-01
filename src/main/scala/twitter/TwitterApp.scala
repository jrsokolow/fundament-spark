package twitter

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import twitter.analysers.{TweetsAnalyser, TweetsSearch}
import twitter.cleaners.TweetsCleaner
import twitter.loaders.TweetsLoader

object TwitterApp {
  def main(args: Array[String]): Unit = {
    def spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local[*]")
      .getOrCreate()

    val tweetsLoader: TweetsLoader  = new TweetsLoader(spark)
    val tweetsCleaner: TweetsCleaner = new TweetsCleaner(spark)
    val tweetsSearch: TweetsSearch = new TweetsSearch(spark)
    val tweetsAnalyser: TweetsAnalyser = new TweetsAnalyser(spark)

    val tweetsDF: Dataset[Row] = tweetsLoader.loadALlTweets().cache()
    val tweetsCleanedDF: Dataset[Row] = tweetsCleaner.cleanAllTweets(tweetsDF)

    import tweetsSearch._

    val trumpTweetsDF: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Trump"))
      .transform(onlyInLocation("United States"))

    val sourceCount: Dataset[Row] = tweetsAnalyser.calculateSourceCount(trumpTweetsDF)

    sourceCount.show()
  }
}
