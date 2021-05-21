package bigquery_stream
import org.apache.spark.sql.DataFrame

object Util {
  def writeToFile(df:DataFrame, filePath:String): Unit = {
    df.coalesce(1)
      .write
      .option("header","true")
      .option("delimiter", "\t")
      .format("csv")
      .mode("overwrite")
      .save(filePath)
  }
}
