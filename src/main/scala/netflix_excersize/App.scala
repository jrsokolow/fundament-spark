package netflix_excersize

import netflix_excersize.udfs.FilmDescriptionLengthUDF
import org.apache.spark.sql.functions.{avg, call_udf, col, explode, split}
import org.apache.spark.sql.types.{DataTypes, IntegerType}
import org.apache.spark.sql.{SparkSession, functions}

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local").getOrCreate()

    import spark.implicits._

    val netflixTitles = spark.read.option("header", "true").csv("src/main/scala/test_data/netflix_titles.csv")
    netflixTitles.printSchema()

    val netflixTitlesWithoutNull = netflixTitles.na.fill("NULL")
//    netflixTitlesWithoutNull.show(1000000)

    val titlesGroupedByTypeCount = netflixTitlesWithoutNull.groupBy("type").count()
//    titlesGroupedByTypeCount.show(1000000)

    val titlesGroupedByDirectors = netflixTitlesWithoutNull.groupBy("director").count()
//    titlesGroupedByDirectors.show(false)

    val titlesGroupedYears = netflixTitlesWithoutNull.groupBy("release_year").count().orderBy("release_year")
//    titlesGroupedYears.show(100000)

    val exploadedListedIn = netflixTitlesWithoutNull
      .withColumn("listed_in", split(col("listed_in"), ","))
      .select(col("show_id"), explode(col("listed_in")).as("singleListedIn"))

//    exploadedListedIn.show(100000000)

    val filtered = exploadedListedIn.groupBy("singleListedIn").count()

//    filtered.show(100000)

    val filmDescriptionLengthUDF: FilmDescriptionLengthUDF = new FilmDescriptionLengthUDF()
    spark.udf.register("filmDescriptionLengthUDF", filmDescriptionLengthUDF, DataTypes.IntegerType)

    val filmDescriptionLengthDataFrame = netflixTitlesWithoutNull.withColumn("filmDescriptionLength",
      call_udf("filmDescriptionLengthUDF", col("description")))

//    filmDescriptionLengthDataFrame.show(false)

    println(filmDescriptionLengthDataFrame.agg(avg("filmDescriptionLength")).first().getDouble(0))

  }

}
