package org.example.habrcareer.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class HabrExtractor(
                   apiBaseUrl: String,
                   vacsPageLimit: Int,
                   vacsPerPage: Int,
                   netRepartition: Int,
                   rawPartition: Int
                   ) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {
      import spark.implicits._

      val totalVacs: Int = math.min(vacsPageLimit * vacsPerPage, {
        val body: String = webService.readOrDefault(
          HabrExtractor.vacanciesURL(apiBaseUrl, 1, 0), """"totalPages":0"""
        )
        """"totalPages"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt
      })

      val urlsDS: Dataset[String] = (0 to (totalVacs / vacsPerPage))
        .map(page => HabrExtractor.vacanciesURL(apiBaseUrl, vacsPerPage, page))
        .toDS()
        .repartition(netRepartition)

      urlsDS.mapPartitions(part => part.flatMap(url => {
        webService.readOrNone(url)
      })).repartition(rawPartition)
    }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame =
    idsDF
}

private object HabrExtractor {
  private def vacanciesURL(apiBaseUrl: String, perPage: Int, page: Int): String =
    s"$apiBaseUrl/frontend/vacancies?sort=date&type=all&per_page=$perPage&page=$page"
}