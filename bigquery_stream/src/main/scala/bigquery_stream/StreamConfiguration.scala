package bigquery_stream

trait StreamConfiguration {
  val sparkJar = "spark.jars.packages"
  val bigQueryDependencies = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1"
  val credentialFilePath = "src/main/resources/bigquery_stream/credentials2.json"
  val baseQuery = "bigquery-public-data:stackoverflow."
  val resourcePath = "src/main/resources/bigquery_stream/"
}
