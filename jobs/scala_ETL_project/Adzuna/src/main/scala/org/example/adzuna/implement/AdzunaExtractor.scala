package org.example.adzuna.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.adzuna.config.AdzunaApiParams
import org.example.adzuna.implement.AdzunaExtractor.clusterURL
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class AdzunaExtractor(
                     apiBaseUrl: String,
                     api: AdzunaApiParams,
                     pageLimit: Int,
                     netPartition: Int,
                     rawPartition: Int
                     ) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {
      import spark.implicits._

      val clusterURLs = (1 to pageLimit)
        .map(page => clusterURL(apiBaseUrl, api, page))
        .toDS().repartition(netPartition)

      clusterURLs
        .mapPartitions(part => part.flatMap(url => webService.readOrNone(url)))
        .repartition(rawPartition)
    }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame = ???
}

object AdzunaExtractor {

  private def clusterURL(
                          apiBaseURL: String,
                          api: AdzunaApiParams,
                          page: Int,
                          vacsPerPage: Int = -1
                        ): String = {

    val perPage = if (vacsPerPage < 0) api.vacsPerPage else vacsPerPage

    s"$apiBaseURL/jobs/${api.locationTag}/search/$page?" +
      s"app_id=${api.appId}&" +
      s"app_key=${api.appKey}&" +
      s"max_days_old=${api.maxDaysOld}&" +
      s"category=${api.categoryTag}&" +
      "sort_by=date&" +
      s"results_per_page=$perPage"
  }

}
