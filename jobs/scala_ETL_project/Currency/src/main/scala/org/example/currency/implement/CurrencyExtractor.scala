package org.example.currency.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class CurrencyExtractor(apiBaseUrl: String, apiKey: String) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {
      import spark.implicits._

      val body = webService.readOrThrow(CurrencyExtractor.URL(apiBaseUrl, apiKey))
        .replace("\n", "")

      Seq(body).toDS()
    }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame = ???
}

private object CurrencyExtractor {
  private def URL(apiBaseUrl: String, apiKey: String): String =
    s"$apiBaseUrl/$apiKey/latest/RUB"
}
