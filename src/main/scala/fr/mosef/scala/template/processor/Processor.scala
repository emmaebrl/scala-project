package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  /**
   * Méthode principale pour générer un rapport en fonction du type demandé
   * @param inputDF le DataFrame d'entrée
   * @param reportType le nom du rapport à générer ("report1", "report2", "report3")
   * @return un DataFrame contenant le rapport
   */
  def process(inputDF: DataFrame, reportType: String): DataFrame

}
