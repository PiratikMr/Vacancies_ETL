package org.example.adzuna.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.adzuna.config.AdzunaApiParams
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class AdzunaExtractor(
                     apiBaseUrl: String,
                     api: AdzunaApiParams,
                     pageLimit: Int
                     ) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {



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
      s"results_per_page=$perPage&" +
      s"max_days_old=${api.maxDaysOld}" +
      s"app_id=${api.appId}&" +
      s"app_key=${api.appKey}&" +
      s"category=${api.categoryTag}"
  }

}
