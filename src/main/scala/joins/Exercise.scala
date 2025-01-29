package joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Exercise {

  def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local")
        .getOrCreate()

        import spark.implicits._

        val people: Seq[(String, String, String, Int)] = Seq(
          ("1", "Arek", "ArielMakoszaRowerzysta", 15),
          ("2", "Zbyszek", "Byczek", 23),
          ("3", "Marek", "Korek", 3),
          ("4", "Sasza", "Mana", 47),
          ("5", "Kamil", "Maka", 67),
          ("6", "Monika", "Ariel", 11),
          ("7", "Anna", "Byczek", 12),
          ("8", "Sylwia", "Korek", 33),
          ("9", "Ewa", "Mana", 49),
          ("10", "Nina", "Maka", 88)
        )

        val jobsDF:Dataset[Row] = Seq(("programmer", 0), ("teacher", 18), ("senator", 30),
          ("president", 35)).toDF("job", "ageLimit")

        val peopleDF:Dataset[Row] = people.toDF("id", "firstName", "lastName", "age") 

        val peopleWithJobs:Dataset[Row] = peopleDF.join(jobsDF, length(peopleDF("firstName")) +
          length(peopleDF("lastName")) <=
          jobsDF("ageLimit"))

        peopleWithJobs.show()

  }

}
