package org.example.core.etl

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.web.WebAdapter

trait Extractor {

  def extract(spark: SparkSession, webService: WebAdapter): Dataset[String]

  def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebAdapter): DataFrame

}