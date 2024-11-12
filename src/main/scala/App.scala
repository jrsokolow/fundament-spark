import model.Person
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local").getOrCreate()

    import spark.implicits._

    val people: Seq[(String, String, String, Integer, String, Double)] = Seq(
      ("marek", "czuma", "111111111", 10, "M", 10000),
      ("marek", "czuma", "222222222", 20, "M", 20000),
      ("marek", "czuma", "333333333", 30, "M", 30000),
      ("marek", "czuma", "444444444", 40, "M", 40000),
    )

    val peopleDF:Dataset[Row] = people.toDF("firstName", "lastName", "personalNumber", "age",
      "sex", "income")

    peopleDF.show()
    peopleDF.select("firstName", "sex", "income").show()
  }

}
