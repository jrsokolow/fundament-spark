import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local").getOrCreate()

    import spark.implicits._

    val people: Seq[(String, String, Int)] = Seq(
      ("marek", "czuma", 42),
      ("atos", "aramis", 10),
      ("kot", "mlot", 100),
      ("reks", "kleks", 149)
    )

    val peopleDF:Dataset[Row] = people.toDF("firstName", "lastName", "age")

    peopleDF.show()
    peopleDF.select("firstName", "lastName").show()
  }

}
