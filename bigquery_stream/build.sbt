name := "bigquery_stream"

version := "0.1"

scalaVersion := "2.12.8"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "3.1.1"),
  ("org.apache.spark" %% "spark-sql" % "3.1.1"),
  ("com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.19.1"),
  ("com.databricks" %% "spark-xml" % "0.11.0")
  )

