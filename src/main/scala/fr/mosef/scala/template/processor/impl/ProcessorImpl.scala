package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.functions.col

class ProcessorImpl extends Processor {

  override def process(inputDF: DataFrame, reportType: String): DataFrame = {
    reportType match {
      case "report1" => countOccurrencesByBrand(inputDF)
      case "report2" => countSubcategoriesPerCategory(inputDF)
      case "report3" => extractToxicRiskRecalls(inputDF)
      case _ => throw new IllegalArgumentException(s"Type de rapport inconnu : $reportType")
    }
  }

  def countOccurrencesByBrand(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("nom_de_la_marque_du_produit")
      .count()
      .orderBy(F.desc("count"))
  }

  def countSubcategoriesPerCategory(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("categorie_de_produit")
      .agg(F.countDistinct("sous_categorie_de_produit").alias("nb_sous_categories"))
      .orderBy(F.desc("nb_sous_categories"))
  }

  def extractToxicRiskRecalls(inputDF: DataFrame): DataFrame = {
    inputDF.filter(
      F.col("motif_du_rappel").contains("plomb") ||
        F.col("motif_du_rappel").contains("pesticide") ||
        F.col("description_complementaire_du_risque").contains("oxyde")
    )
  }
}