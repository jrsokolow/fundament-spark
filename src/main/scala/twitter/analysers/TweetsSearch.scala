package twitter.analysers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TweetsSearch {
  private val TEXT: String = "text"
  private val USER_LOCATION: String = "user_location"
}

class TweetsSearch(sparkSession: SparkSession) {
  def searchByKeyWord(keyWord: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(TweetsSearch.TEXT).contains(keyWord))
  }

  def searchByKeyWords(keyWords: Seq[String])(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("keyWordsResult", array_intersect(split(col(TweetsSearch.TEXT), " "), split(lit
    (keyWords.mkString(",")), " ")))
      .filter(!(col("keyWordsResult").isNull.or(size(col("keyWordsResult")).equalTo(0))))
      .drop("keyWordsResult")
  }

  def onlyInLocation(location: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(TweetsSearch.USER_LOCATION).contains(location))
  }
}
