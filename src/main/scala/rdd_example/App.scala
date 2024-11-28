package rdd_example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("fundament-spark").master("local")
      .getOrCreate();

//    val numbersList: Seq[Int] = Seq(10,20,30,40,50,60)
//    val numbersRDD: RDD[Int] = spark.sparkContext.parallelize(numbersList)
//    val sum: Int = numbersRDD.reduce((v1, v2)=> v1*v2)
//    println(sum)

    val namesList: Seq[String] = Seq("marek", "ania", "kania", "ignas", "leszek")
    val namesRDD: RDD[String] = spark.sparkContext.parallelize(namesList)
    val namesBig: RDD[String] = namesRDD.map(n=>n.toUpperCase)
    namesBig.take(5).foreach(println)

  }

}
