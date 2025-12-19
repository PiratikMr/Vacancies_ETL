package org.example.geekjob.implement

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.WebService

class GeekJobExtractor(
                      apiBaseUrl: String,
                      pageLimit: Int,
                      netPartition: Int,
                      rawPartitions: Int
                      ) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
    {

      import spark.implicits._

      val _apiBaseUrl = apiBaseUrl

      val firstPageContent: String = webService.readOrThrow(GeekJobExtractor.pageURL(_apiBaseUrl, 1))

      val pageURLs: Dataset[String] = {
        val totalPages: Int = """<small>страниц (\d+)</small>""".r.findFirstMatchIn(firstPageContent).get.group(1).toInt
        val pagesToProcess: Int = math.min(totalPages, pageLimit)

        (2 to pagesToProcess).map(page => GeekJobExtractor.pageURL(_apiBaseUrl, page))
      }.toDS

      val vacancyIDs: Dataset[String] = pageURLs.repartition(netPartition)
        .mapPartitions(part => part.flatMap(url => {
          webService.readOrNone(url) match {
            case Some(body) => """/vacancy/([a-z0-9]{24})""".r.findAllMatchIn(body).map(_.group(1))
            case None => None
          }
        })).dropDuplicates()


      vacancyIDs.mapPartitions(part => part.flatMap(id => {
        webService.readOrNone(GeekJobExtractor.vacURL(_apiBaseUrl, id)) match {
          case Some(body) =>
            val start: Int = body.indexOf("""<article class="row vacancy">""")
            val bodyEnd: String = "</article>"
            val end: Int = body.indexOf(bodyEnd, start) + bodyEnd.length

            Some(id + body.substring(start, end).replace("\n", ""))
          case None => None
        }
      })).repartition(rawPartitions)

    }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame =
    {
      import spark.implicits._

      val _apiBaseUrl = apiBaseUrl

      idsDF.repartition(netPartition)
        .mapPartitions(part => part.flatMap(row =>
          webService.readOrNone(GeekJobExtractor.vacURL(_apiBaseUrl, row.getString(0))) match {
            case Some(body) if body.contains("Эта вакансия была перемещена в архив.") => Some(row.getString(0))
            case _ => None
          }
        )).toDF("id")
    }
}

private object GeekJobExtractor {

  private def pageURL(apiBaseUrl: String, page: Int): String = s"$apiBaseUrl/vacancies/$page"

  private def vacURL(apiBaseUrl: String, code: String): String = s"$apiBaseUrl/vacancy/$code"
}
