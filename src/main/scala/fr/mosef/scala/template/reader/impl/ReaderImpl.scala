package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def readCSV(path: String, delimiter: String = ";", header: Boolean = true, schema: Option[StructType] = None): DataFrame = {
    val reader = sparkSession
      .read
      .option("sep", delimiter)
      .option("header", header.toString)
      .option("inferSchema", schema.isEmpty.toString)

    val finalReader = schema.map(reader.schema).getOrElse(reader)

    finalReader
      .format("csv")
      .load(path)
  }

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .parquet(path)
  }

  def readHiveTable(tableName: String): DataFrame = {
    sparkSession
      .table(tableName)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }
}
