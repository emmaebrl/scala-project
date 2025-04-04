package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame, reportType: String): DataFrame = {
    reportType match {
      case "report1" => generateReport1(inputDF)
      case "report2" => generateReport2(inputDF)
      case "report3" => generateReport3(inputDF)
      case _ =>
        throw new IllegalArgumentException(s"Rapport inconnu: $reportType")
    }
  }

  def generateReport1(inputDF: DataFrame): DataFrame = {
    // Rapport 1 : nombre d’occurrences par groupe
    inputDF.groupBy("group_key").count()
  }

  def generateReport2(inputDF: DataFrame): DataFrame = {
    // Rapport 2 : somme d’un champ numérique par groupe
    inputDF.groupBy("group_key").agg(F.sum("field1").as("total_field1"))
  }

  def generateReport3(inputDF: DataFrame): DataFrame = {
    // Rapport 3 : transformation sans groupBy (ex : filtre ou enrichissement)
    inputDF.filter(F.col("field1") > 100)
  }

}
