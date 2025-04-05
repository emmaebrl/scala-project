package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

class Writer(spark: SparkSession, props: Properties) {
  def parseSaveMode(mode: String): SaveMode = mode.toLowerCase match {
    case "overwrite"     => SaveMode.Overwrite
    case "append"        => SaveMode.Append
    case "ignore"        => SaveMode.Ignore
    case "errorifexists" => SaveMode.ErrorIfExists
    case other => throw new IllegalArgumentException(s"Mode d'écriture non supporté: $other")
  }


  def write(df: DataFrame, path: String): Unit = {
    val format    = props.getProperty("format", "csv")
    val mode      = props.getProperty("mode", "overwrite")
    val coalesce  = props.getProperty("coalesce", "false").toBoolean
    val header    = props.getProperty("header", "true")
    val delimiter = props.getProperty("separator", ",")

    val finalDF = if (coalesce) df.coalesce(1) else df

    val writer = finalDF.write
      .option("header", header)
      .option("sep", delimiter)
      .mode(parseSaveMode(mode))


    format match {
      case "csv"     => writer.csv(path)
      case "parquet" => writer.parquet(path)
      case "json"    => writer.json(path)
      case _         => throw new IllegalArgumentException(s"Format de sortie inconnu : $format")
    }
  }

  def showPreview(df: DataFrame, numRows: Int = 10): Unit = {
    df.show(numRows, truncate = false)
  }
}
