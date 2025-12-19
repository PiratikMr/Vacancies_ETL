package org.example.headhunter.dictionaries.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class HHExtractor(url: String) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {
      import spark.implicits._

      val body = webService.readOrDefault(url, "")
      Seq(body).toDS
    }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame = ???
}
