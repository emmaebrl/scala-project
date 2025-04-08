package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.schemas.CsvSchemas  // 👈 import du schéma
import scala.util.{Try, Success, Failure}
import java.io.File

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def readCSV(path: String, delimiter: String = ",", header: Boolean = true, schema: Option[StructType] = None): DataFrame = {
    validatePath(path)
    validateFileExtension(path, List(".csv", ".txt", ".data"))

    Try {
      val options = Map(
        "sep" -> delimiter,
        "header" -> header.toString,
        "inferSchema" -> schema.isEmpty.toString,
        "mode" -> "FAILFAST",
        "quote" -> "\"",
        "multiline" -> "true"
      )

      val reader = sparkSession.read.options(options)
      val withSchema = schema.map(reader.schema).getOrElse(reader)

      withSchema.format("csv").load(path)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading CSV file $path: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read CSV: ${e.getMessage}", e)
    }
  }

  // ✅ Nouvelle méthode : lecture CSV avec le schéma de rappel
  def readRappelCSV(path: String, delimiter: String = ","): DataFrame = {
    readCSV(path, delimiter, header = true, schema = Some(CsvSchemas.rappelSchema))
  }

  def readParquet(path: String): DataFrame = {
    validatePath(path)
    validateFileExtension(path, List(".parquet", ".pqt"))

    Try {
      sparkSession.read.parquet(path)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading Parquet file $path: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read Parquet: ${e.getMessage}", e)
    }
  }

  def readHiveTable(tableName: String): DataFrame = {
    if (tableName == null || tableName.trim.isEmpty) {
      throw new IllegalArgumentException("Table name cannot be null or empty")
    }

    Try {
      sparkSession.table(tableName)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading Hive table $tableName: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read Hive table: ${e.getMessage}", e)
    }
  }

  private def validatePath(path: String): Unit = {
    if (path == null || path.trim.isEmpty) {
      throw new IllegalArgumentException("Path cannot be null or empty")
    }

    if (!path.matches("^(hdfs://|s3a://|gs://|wasbs://).*")) {
      val file = new File(path)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Path $path does not exist")
      }
    }
  }

  private def validateFileExtension(path: String, validExtensions: List[String]): Unit = {
    val isValid = validExtensions.exists(ext => path.toLowerCase.endsWith(ext))
    if (!isValid) {
      logWarning(s"File $path does not have one of the expected extensions: ${validExtensions.mkString(", ")}")
    }
  }

  private def logError(message: String): Unit = {
    println(s"[ERROR] $message")
  }

  private def logWarning(message: String): Unit = {
    println(s"[WARNING] $message")
  }
}
