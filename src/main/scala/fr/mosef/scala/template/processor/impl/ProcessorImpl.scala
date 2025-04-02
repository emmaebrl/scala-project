package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}

class ProcessorImpl extends Processor {

  // Rapport 1 : Comptage par champ
  override def process(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("group_key").count()
  }

  // Rapport 2 : Statistiques descriptives
  override def computeSummary(inputDF: DataFrame): DataFrame = {
    inputDF.describe()
  }

  // Rapport 3 : Moyenne ou autre aggregation par un champ donné
  override def groupByField(inputDF: DataFrame, field: String): DataFrame = {
    inputDF.groupBy(field).agg(F.avg("score").alias("avg_score"))  // adapte "score" si besoin
  }
}
