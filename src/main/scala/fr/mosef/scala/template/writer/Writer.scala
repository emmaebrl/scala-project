package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Writer(sparkSession: SparkSession) {

  // Écriture générique avec format et options custom
  def write(df: DataFrame, path: String, format: String = "csv", mode: String = "overwrite", options: Map[String, String] = Map.empty): Unit = {
    df.write
      .options(options)
      .mode(mode)
      .format(format)
      .save(path)
  }

  // Écriture CSV (coalesce pour un seul fichier, séparateur customisable)
  def writeCSV(df: DataFrame, outputPath: String, delimiter: String = ",", header: Boolean = true): Unit = {
    df.coalesce(1)
      .write
      .option("header", header.toString)
      .option("sep", delimiter)
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }

  // Écriture Parquet
  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }

  // Écriture table Hive
  def writeHiveTable(df: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write
      .mode(mode)
      .saveAsTable(tableName)
  }

  // Aperçu du DataFrame
  def showPreview(df: DataFrame, numRows: Int = 10): Unit = {
    df.show(numRows, truncate = false)
  }
}
