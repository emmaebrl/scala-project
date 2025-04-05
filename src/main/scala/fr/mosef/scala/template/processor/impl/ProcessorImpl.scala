package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.functions.col

class ProcessorImpl extends Processor {

  override def process(inputDF: DataFrame, reportType: String): DataFrame = {
    reportType match {
      case "report1" => generateReport1(inputDF)
      case "report2" => generateReport2(inputDF)
      case "report3" => generateReport3(inputDF)
      case _ =>
        throw new IllegalArgumentException(s"Type de rapport inconnu : $reportType")
    }
  }

  // Rapport 1 : nombre d’occurrences par valeur de "group_key"
  def generateReport1(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("nom_de_la_marque_du_produit")
      .count()
      .orderBy(F.desc("count"))
  }


  // Rapport 2 : somme d’un champ numérique par "group_key"
  def generateReport2(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("categorie_de_produit")
      .count()
      .orderBy(F.desc("count"))
  }


  // Rapport 3 : transformation sans groupBy (ex : filtre sur field1 > 100)
  def generateReport3(inputDF: DataFrame): DataFrame = {
    inputDF.filter(
      F.col("motif_du_rappel").contains("plomb") ||
        F.col("motif_du_rappel").contains("pesticide") ||
        F.col("description_complementaire_du_risque").contains("oxyde")
    )
  }


}