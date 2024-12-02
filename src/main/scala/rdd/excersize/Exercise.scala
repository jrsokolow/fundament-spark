package rdd.excersize

import model.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Exercise {

  def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local")
        .getOrCreate();

        val people: Seq[Person] = Seq(
          Person("Arek", "Ariel", 15, "M"),
          Person("Zbyszek", "Byczek", 23, "M"),
          Person("Marek", "Korek", 3, "M"),
          Person("Sasza", "Mana", 47, "M"),
          Person("Kamil", "Maka", 67, "M"),
          Person("Monika", "Ariel", 11, "F"),
          Person("Anna", "Byczek", 12, "F"),
          Person("Sylwia", "Korek", 33, "F"),
          Person("Ewa", "Mana", 49, "F"),
          Person("Nina", "Maka", 88, "F"),
        )

        val peopleRDD:RDD[Person] = spark.sparkContext.parallelize(people)

        val onlyMale:RDD[Person] = peopleRDD.filter(person => person.sex == "M")
        val allMalesAge: Double = onlyMale.map(_.age).sum()
        println(allMalesAge)

        val onlyFemale:RDD[Person] = peopleRDD.filter(person => person.sex == "F")
        val allFemalesAge: Double = onlyFemale.map(_.age).sum()
        println(allFemalesAge)

        val minAge:Int = peopleRDD.map(_.age).min()
        println(minAge)
        val maxAge:Int = peopleRDD.map(_.age).max()
        println(maxAge)

  }

}
