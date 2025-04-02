package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
      .mode(mode)
      .csv(path)
  }

    def writeCSV(df: DataFrame, outputPath: String): Unit = {
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
    }

    def writeParquet(df: DataFrame, outputPath: String): Unit = {
      df.write.mode("overwrite").parquet(outputPath)
    }

    def showPreview(df: DataFrame, numRows: Int = 10): Unit = {
      df.show(numRows, truncate = false)
    }

}
