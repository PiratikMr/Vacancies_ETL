package org.example.headhunter.implement

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.Interfaces.ETL.Extractor
import org.example.core.Interfaces.Services.{DataBaseService, WebService}

class HHExtractor(
                   dbService: DataBaseService,
                   apiBaseUrl: String,
                   fieldIds: Seq[Int],
                   dateFrom: String,
                   pageLimit: Int,
                   vacsPerPage: Int,
                   netPartition: Int,
                   rawPartition: Int
                 ) extends Extractor {

  override def extract(spark: SparkSession, webService: WebService): Dataset[String] =
  {
    import spark.implicits._

    val _apiBaseUrl = apiBaseUrl
    val _dateFrom = dateFrom
    val _pageLimit = pageLimit
    val _vacsPerPage = vacsPerPage

    val roleIds = dbService.load(spark, "hh_professionalroles")
      .where(col("field_id").isin(fieldIds: _*))
      .select("id")
      .map(row => row.getLong(0))

    val clusterURLs: Dataset[String] = roleIds.mapPartitions(part => part.flatMap(role_id => {
      val body: String = webService.readOrDefault(
        HHExtractor.clusterURL(_apiBaseUrl, _dateFrom, role_id, 0, 0), """{"found":0}"""
      )

      val found: Int = """"found"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt

      if (found == 0) Nil
      else (0 to math.min(_pageLimit, found / _vacsPerPage))
        .map(page => HHExtractor.clusterURL(_apiBaseUrl, _dateFrom, role_id, page, _vacsPerPage))
    }))

    val clusterData: Dataset[String] = clusterURLs.mapPartitions(part => part.flatMap(url =>
      webService.readOrNone(url)
    ))

    val schema: StructType = StructType(Seq(StructField("items", ArrayType(StructType(Seq(StructField("id", StringType)))))))

    val vacURLs: Dataset[String] = spark.read.schema(schema)
      .json(clusterData).select(explode(col("items.id")).as("id")).dropDuplicates("id")
      .map(row => HHExtractor.vacancyURL(_apiBaseUrl, row.getString(0)))

    vacURLs.repartition(netPartition).mapPartitions(part => part.flatMap(url =>
      webService.readOrNone(url)
    )).repartition(rawPartition)
  }

  override def filterUnActiveVacancies(spark: SparkSession, idsDF: DataFrame, webService: WebService): DataFrame =
  {
    import spark.implicits._

    val _apiBaseUrl = apiBaseUrl

    idsDF.mapPartitions(part => part.flatMap(row => {
      val id: Long = row.getLong(0)

      webService.read(HHExtractor.vacancyURL(_apiBaseUrl, s"$id")) match {
        case Right(body) if body.contains(""""archived":true""") => Some(id)
        case Left(error) if error.contains("Status: 404") => Some(id)
        case _ => None
      }
    })).toDF()
  }
}

private object HHExtractor {
  private def clusterURL(apiBaseUrl: String, dateFrom: String, roleId: Long, page: Long, perPage: Long): String =
    s"$apiBaseUrl/vacancies?page=$page&per_page=$perPage&professional_role=$roleId&date_from=$dateFrom"

  private def vacancyURL(apiBaseUrl: String, id: String): String = s"$apiBaseUrl/vacancies/$id"
}