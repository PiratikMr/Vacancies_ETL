package com.files

import com.files.Common.URLConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.rogach.scallop.ScallopOption


object ExtractVacancies extends SparkApp {

  private def vacancyURL(id: String): String = s"https://api.hh.ru/vacancies/$id"

  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      val dateFrom: ScallopOption[String] = opt[String](name = "datefrom")

      lazy val fieldIds: Seq[Int] = getFromConfFile[Seq[Int]]("fieldIds")
      lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
      lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
      lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

      verify()
    }
    val spark: SparkSession = defineSession(conf.sparkConf)

    // parameters
    val dateFrom: String = conf.dateFrom()
    val urlConf: URLConf = conf.urlConf


    val clusterURL: (Long, Long, Long) => String = (id, page, perPage) =>
      s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id&date_from=$dateFrom"


    val ids: Seq[Int] = conf.fieldIds
    val vacsPP: Int = conf.vacsPerPage
    val pageLimit: Int = conf.pageLimit


    import spark.implicits._


    val fieldIds: Dataset[Long] = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Roles))
      .where(col("parent_id").isin(ids: _*)).select("id").map(row => row.getLong(0))

    val clusterURLs: Dataset[String] = fieldIds.mapPartitions(part => {

      URLHandler.useClient[Long, String](part, (backend, role_id) => {
        val body: String = URLHandler.readOrDefault(urlConf, clusterURL(role_id, 0, 0), """{"found":0}""", Some(backend))

        val found: Int = """"found"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt

        if (found == 0) Nil
        else (0 to math.min(pageLimit, found / vacsPP)).map(page => clusterURL(role_id, page, vacsPP))
      })

    })

    val clusterData: Dataset[String] = clusterURLs.mapPartitions(part => {
      URLHandler.useClient[String, String](part, (backend, url) => URLHandler.readOrNone(urlConf, url, Some(backend)))
    })

    val schema: StructType = StructType(Seq(StructField("items", ArrayType(StructType(Seq(StructField("id", StringType)))))))
    val vacURLs: Dataset[String] = spark.read.schema(schema)
      .json(clusterData).select(explode(col("items.id")).as("id")).dropDuplicates("id").map(row => vacancyURL(row.getString(0)))

    val finalData: Dataset[String] = vacURLs.repartition(urlConf.requestsPS).mapPartitions(part => {
      URLHandler.useClient[String, String](part, (backend, url) => URLHandler.readOrNone(urlConf, url, Some(backend)))
    })

    finalData.repartition(conf.rawPartitions).write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

    spark.stop()
  }
}