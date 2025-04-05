package fr.mosef.scala.template.reader

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types.StructType

trait Reader {

  def readCSV(path: String, delimiter: String = ",", header: Boolean = true, schema: Option[StructType] = None): DataFrame

  def readParquet(path: String): DataFrame

  def readHiveTable(tableName: String): DataFrame

}
