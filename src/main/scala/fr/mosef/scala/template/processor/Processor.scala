package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  def process(inputDF: DataFrame): DataFrame

  def generateReport1(inputDF: DataFrame): DataFrame

  def generateReport2(inputDF: DataFrame): DataFrame

  def generateReport3(inputDF: DataFrame): DataFrame

}
