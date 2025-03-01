package twitter.analysers

import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TweetsAnalyser {
  private val HASHTAG_COLUMN: String = "hashtag"
  private val IS_RETWEET_COLUMN: String = "is_retweet"
  val SOURCE_COLUMN: String = "source"
  val USER_FOLLOWERS: String = "user_followers"
  val USER_NAME: String = "user_name"
  val USER_LOCATION: String = "user_location"
}

class TweetsAnalyser(sparkSession: SparkSession) {

  /**
   * AGGREGATION
   * @param df
   * @return Dataframe with columns hashtag, count
   */
  def calculateHashtags(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn(TweetsAnalyser.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyser.HASHTAG_COLUMN)))
      .groupBy(TweetsAnalyser.HASHTAG_COLUMN).count()
  }

  /**
   * AGGREGATION
   * @param df
   * @return Dataframe with columns is_retweet, count
   */
  def calculateIsRetweetCount(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(TweetsAnalyser.IS_RETWEET_COLUMN).count()
  }

  def calculateSourceCount(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(TweetsAnalyser.SOURCE_COLUMN).count()
  }

  /**
   * AGGREGATION
   * @param df
   * @return Dataframe with columns user_location, avg
   */
  def calculateAvgUserFollowersPerLocation(df: Dataset[Row]): Dataset[Row] = {
    df.select(TweetsAnalyser.USER_NAME, TweetsAnalyser.USER_FOLLOWERS, TweetsAnalyser.USER_LOCATION)
      .filter(col(TweetsAnalyser.USER_NAME).isNotNull)
      .filter(col(TweetsAnalyser.USER_LOCATION).isNotNull)
      .dropDuplicates(TweetsAnalyser.USER_NAME)
      .groupBy(TweetsAnalyser.USER_LOCATION)
      .avg(TweetsAnalyser.USER_FOLLOWERS)
      .as("avg")
  }

}
