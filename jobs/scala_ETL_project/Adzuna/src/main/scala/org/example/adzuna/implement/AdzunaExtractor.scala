package org.example.adzuna.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class AdzunaExtractor extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] = ???

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame = ???
}
