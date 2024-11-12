import model.Person
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local").getOrCreate()

    import spark.implicits._

//  Using DataFrame
    val people: Seq[(String, String, String, Integer, String, Double)] = Seq(
        ("Arek", "Ariel", "111111111", 10, "M", 10000),
        ("Zbyszek", "Byczek", "222222222", 20, "M", 20000),
        ("Marek", "Korek", "333333333", 30, "M", 30000),
        ("Sasza", "Mana", "444444444", 40, "M", 40000),
    )
    val peopleSet:DataFrame = people.toDF("firstName", "lastName", "personalNumber", "age",
      "sex", "income")

//    Use strongly typed DataSet[Person]
//    val people: Seq[Person] = Seq(
//      Person("Arek", "Ariel", "111111111", 10, "M", 10000),
//      Person("Zbyszek", "Byczek", "222222222", 20, "M", 20000),
//      Person("Marek", "Korek", "333333333", 30, "M", 30000),
//      Person("Sasza", "Mana", "444444444", 40, "M", 40000),
//    )
//    val peopleSet:Dataset[Person] = people.toDS()

    peopleSet.show()
    peopleSet.select("firstName", "sex", "income").show()
  }

}
