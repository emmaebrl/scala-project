package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import fr.mosef.scala.template.reader.Reader
import java.util.Properties
import org.apache.spark.sql.streaming.StreamingQuery
import java.io.File
import scala.util.{Try, Success, Failure}

/**
 * Robust implementation of the Reader interface.
 * Provides methods to read data in different formats with error handling
 * and validation.
 *
 * @param sparkSession Spark session used to read data
 */
class ReaderImpl(sparkSession: SparkSession) extends Reader {

  /**
   * Reads a dataset with the specified format and provided options
   *
   * @param format Data format (e.g. "csv", "parquet", "json")
   * @param options Reading options
   * @param path File or directory path
   * @return DataFrame containing the data
   */
  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    validatePath(path)

    Try {
      sparkSession
        .read
        .options(options)
        .format(format)
        .load(path)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading file $path in format $format: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read in format $format: ${e.getMessage}", e)
    }
  }

  /**
   * Reads a CSV file with configurable options
   *
   * @param path CSV file path
   * @param delimiter Column separator
   * @param header Indicates if the first line contains headers
   * @param schema Optional schema to apply
   * @return DataFrame containing CSV data
   */
  def readCSV(path: String, delimiter: String = ",", header: Boolean = true, schema: Option[StructType] = None): DataFrame = {
    validatePath(path)
    validateFileExtension(path, List(".csv", ".txt", ".data"))

    Try {
      val options = Map(
        "sep" -> delimiter,
        "header" -> header.toString,
        "inferSchema" -> schema.isEmpty.toString,
        "mode" -> "FAILFAST" // Fails quickly if there's an error in the data
      )

      val reader = sparkSession.read.options(options)
      val withSchema = schema.map(reader.schema).getOrElse(reader)

      withSchema.format("csv").load(path)
    } match {
      case Success(df) =>
        // Additional data quality check
        if (df.count() == 0) {
          logWarning(s"CSV file $path was successfully read but contains no data")
        }
        df
      case Failure(e) =>
        logError(s"Error reading CSV file $path: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read CSV: ${e.getMessage}", e)
    }
  }

  /**
   * Reads a Parquet file
   *
   * @param path Parquet file path
   * @return DataFrame containing Parquet data
   */
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

  /**
   * Reads a Hive table
   *
   * @param tableName Hive table name
   * @return DataFrame containing table data
   */
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

  /**
   * Reads a JSON file
   *
   * @param path JSON file path
   * @param schema Optional schema to apply
   * @return DataFrame containing JSON data
   */
  def readJSON(path: String, schema: Option[StructType] = None): DataFrame = {
    validatePath(path)
    validateFileExtension(path, List(".json", ".jsonl"))

    Try {
      val reader = sparkSession.read
        .option("multiLine", "true") // Supports multi-line JSON

      val withSchema = schema.map(reader.schema).getOrElse(reader)
      withSchema.json(path)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading JSON file $path: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read JSON: ${e.getMessage}", e)
    }
  }

  /**
   * Reads data from a JDBC source
   *
   * @param url JDBC connection URL
   * @param table Table name or query
   * @param properties Connection properties (credentials, etc.)
   * @return DataFrame containing the data
   */
  def readJDBC(url: String, table: String, properties: Properties): DataFrame = {
    if (url == null || url.trim.isEmpty) {
      throw new IllegalArgumentException("JDBC URL cannot be null or empty")
    }

    if (table == null || table.trim.isEmpty) {
      throw new IllegalArgumentException("Table name cannot be null or empty")
    }

    Try {
      sparkSession.read.jdbc(url, table, properties)
    } match {
      case Success(df) => df
      case Failure(e) =>
        // Mask sensitive information in error message
        val safeUrl = url.replaceAll("(password|pwd)=[^&]*", "$1=***")
        logError(s"Error connecting to JDBC at $safeUrl: ${e.getMessage}")
        throw new RuntimeException(s"Failed to connect to JDBC: ${e.getMessage}", e)
    }
  }

  /**
   * Reads an ORC format file
   *
   * @param path ORC file path
   * @return DataFrame containing ORC data
   */
  def readORC(path: String): DataFrame = {
    validatePath(path)
    validateFileExtension(path, List(".orc"))

    Try {
      sparkSession.read.orc(path)
    } match {
      case Success(df) => df
      case Failure(e) =>
        logError(s"Error reading ORC file $path: ${e.getMessage}")
        throw new RuntimeException(s"Failed to read ORC: ${e.getMessage}", e)
    }
  }

  /**
   * Simplified method to read a CSV with default parameters
   *
   * @param path File path
   * @return DataFrame containing the data
   */
  def read(path: String): DataFrame = {
    // Determines format based on extension
    val format = detectFormat(path)

    format match {
      case "csv" => readCSV(path)
      case "parquet" => readParquet(path)
      case "json" => readJSON(path)
      case "orc" => readORC(path)
      case _ =>
        // Default format if not recognized
        readCSV(path)
    }
  }

  /**
   * Detects file format based on extension
   *
   * @param path File path
   * @return Detected format
   */
  private def detectFormat(path: String): String = {
    val lowerPath = path.toLowerCase
    if (lowerPath.endsWith(".csv") || lowerPath.endsWith(".txt")) "csv"
    else if (lowerPath.endsWith(".parquet") || lowerPath.endsWith(".pqt")) "parquet"
    else if (lowerPath.endsWith(".json") || lowerPath.endsWith(".jsonl")) "json"
    else if (lowerPath.endsWith(".orc")) "orc"
    else if (lowerPath.endsWith(".avro")) "avro"
    else "unknown"
  }

  /**
   * Creates an empty DataFrame for testing
   *
   * @return Empty DataFrame
   */
  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation' as message")
  }

  /**
   * Validates that the path exists
   *
   * @param path Path to validate
   */
  private def validatePath(path: String): Unit = {
    if (path == null || path.trim.isEmpty) {
      throw new IllegalArgumentException("Path cannot be null or empty")
    }

    // If the path is a local file (not HDFS/S3/etc.), check that it exists
    if (!path.matches("^(hdfs://|s3a://|gs://|wasbs://).*")) {
      val file = new File(path)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Path $path does not exist")
      }
    }
  }

  /**
   * Validates file extension
   *
   * @param path File path
   * @param validExtensions Valid extensions
   */
  private def validateFileExtension(path: String, validExtensions: List[String]): Unit = {
    val isValid = validExtensions.exists(ext => path.toLowerCase.endsWith(ext))
    if (!isValid) {
      logWarning(s"File $path does not have one of the expected extensions: ${validExtensions.mkString(", ")}")
    }
  }

  /**
   * Logs an error
   *
   * @param message Error message
   */
  private def logError(message: String): Unit = {
    // Replace with your preferred logging system
    println(s"[ERROR] $message")
  }

  /**
   * Logs a warning
   *
   * @param message Warning message
   */
  private def logWarning(message: String): Unit = {
    // Replace with your preferred logging system
    println(s"[WARNING] $message")
  }
}