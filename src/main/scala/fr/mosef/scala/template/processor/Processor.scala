package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {"TO THE MOON "

  def process(inputDF: DataFrame) : DataFrame

}
