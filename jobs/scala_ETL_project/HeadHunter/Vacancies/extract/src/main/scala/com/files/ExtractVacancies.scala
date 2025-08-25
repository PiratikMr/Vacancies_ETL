package com.files

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.rogach.scallop.ScallopOption


object ExtractVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val fieldIds: Seq[Int] = getFromConfFile[Seq[Int]]("fieldIds")
    lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
    lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    val dateFrom: ScallopOption[String] = opt[String](name = "datefrom")

    define()
  }
  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf, conf.urlConf.requestsPS)

  private def clusterURL(id: Long, page: Long = 0, perPage: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id&date_from=${conf.dateFrom()}"

  private def vacUrl(id: String): String = s"https://api.hh.ru/vacancies/$id"


  import spark.implicits._


  private val ids: Dataset[Long] = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Roles))
    .where(col("parent_id").isin(conf.fieldIds: _*)).select("id").map(row => row.getLong(0)).repartition(6)

  private val clusterData: Dataset[String] = ids.mapPartitions(partition => {
    partition.flatMap(id => {
      val data: String = URLHandler.readOrDefault(clusterURL(id), conf.urlConf, """{"found":0}""")

      val found: Long = """"found"\s*:\s*(\d+)""".r.findFirstMatchIn(data).get.group(1).toLong

      if (found <= 0) Nil
      else {
        val pages: Long = math.min(found / conf.vacsPerPage, conf.pageLimit)
        (0L to pages).map(page => clusterURL(id, page, conf.vacsPerPage))
      }
    })
  }).mapPartitions(urls => urls.flatMap(url => URLHandler.readOrNone(url, conf.urlConf)))

  private val schema: StructType = StructType(Seq(StructField("items", ArrayType(StructType(Seq(StructField("id", StringType)))))))
  private val vacIdsDS: Dataset[String] = spark.read.schema(schema)
    .json(clusterData).select(explode(col("items.id")).as("id")).dropDuplicates("id").map(row => vacUrl(row.getString(0)))


  private val vacData: Dataset[String] = vacIdsDS.repartition(conf.urlConf.requestsPS).mapPartitions(part => part.flatMap(
    url => URLHandler.readOrNone(url, conf.urlConf)
  ))

  vacData.repartition(conf.rawPartitions).write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

  spark.stop()
}