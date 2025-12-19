package org.example.core.Interfaces.ETL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.Services.WebService

trait Extractor {

  def extract(spark: SparkSession, webService: WebService): Dataset[String]

  def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame

}