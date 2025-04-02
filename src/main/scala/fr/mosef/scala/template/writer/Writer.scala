package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

class Writer {

  // Écriture générique : CSV par défaut, mais modifiable via format + mode
  def write(df: DataFrame, path: String, format: String = "csv", mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .option("header", "true")
      .format(format)
      .save(path)
  }

  // Écriture CSV (spécifique, avec coalesce pour 1 seul fichier)
  def writeCSV(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)
  }

  // Écriture Parquet
  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode("overwrite")
      .parquet(outputPath)
  }

  // Aperçu du DataFrame en console
  def showPreview(df: DataFrame, numRows: Int = 10): Unit = {
    df.show(numRows, truncate = false)
  }
}
