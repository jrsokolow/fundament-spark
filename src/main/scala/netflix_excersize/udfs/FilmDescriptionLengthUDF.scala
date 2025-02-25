package netflix_excersize.udfs

import org.apache.spark.sql.api.java.{UDF1}

class FilmDescriptionLengthUDF extends UDF1[String, Integer] {
  override def call(movieDescription: String): Integer = {
    movieDescription.length
  }
}
