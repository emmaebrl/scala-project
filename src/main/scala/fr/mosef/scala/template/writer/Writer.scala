package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

class Writer(spark: SparkSession, props: Properties) {

  def write(df: DataFrame, path: String): Unit = {
    val format    = props.getProperty("format", "csv")
    val mode      = props.getProperty("mode", "overwrite")
    val coalesce  = props.getProperty("coalesce", "false").toBoolean
    val header    = props.getProperty("header", "true")
    val delimiter = props.getProperty("separator", ",")
    val writer = df.write
      .option("header", header)
      .option("sep", delimiter)
      .mode(SaveMode.valueOf(mode.toUpperCase))

    val finalWriter = if (coalesce) writer.coalesce(1) else writer

    format match {
      case "csv"     => finalWriter.csv(path)
      case "parquet" => finalWriter.parquet(path)
      case "json"    => finalWriter.json(path)
      case _         => throw new IllegalArgumentException(s"Format de sortie inconnu : $format")
    }
  }

  def showPreview(df: DataFrame, numRows: Int = 10): Unit = {
    df.show(numRows, truncate = false)
  }
}
