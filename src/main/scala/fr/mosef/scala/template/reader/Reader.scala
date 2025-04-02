package fr.mosef.scala.template.reader

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types.StructType

trait Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame

  def read(path: String): DataFrame

  def read(): DataFrame

  def readCSV(path: String, delimiter: String = ",", header: Boolean = true, schema: Option[StructType] = None): DataFrame

  def readParquet(path: String): DataFrame

  def readHiveTable(tableName: String): DataFrame

}
