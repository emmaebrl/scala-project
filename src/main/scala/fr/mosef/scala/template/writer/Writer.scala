package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties
import org.apache.spark.sql.functions._

class Writer(spark: SparkSession, props: Properties) {

  def parseSaveMode(mode: String): SaveMode = mode.toLowerCase match {
    case "overwrite"     => SaveMode.Overwrite
    case "append"        => SaveMode.Append
    case "ignore"        => SaveMode.Ignore
    case "errorifexists" => SaveMode.ErrorIfExists
    case other => throw new IllegalArgumentException(s"Mode d'écriture non supporté: $other")
  }

  def sanitizeDataFrame(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(
        colName,
        when(col(colName).isNotNull, regexp_replace(col(colName).cast("string"), "[\\r\\n]+", " "))
          .otherwise(null)
      )
    }
  }

  def write(df: DataFrame, path: String): Unit = {
    val format    = props.getProperty("format", "csv")
    val mode      = props.getProperty("mode", "overwrite")
    val coalesce  = props.getProperty("coalesce", "false").toBoolean
    val header    = props.getProperty("header", "true")
    val delimiter = props.getProperty("separator", ",")

    // Nettoyage : suppression des sauts de ligne dans toutes les colonnes
    val cleanedDF = sanitizeDataFrame(df)
    val finalDF   = if (coalesce) cleanedDF.coalesce(1) else cleanedDF

    val writer = finalDF.write
      .option("header", header)
      .option("sep", delimiter)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("quoteAll", "true")
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
