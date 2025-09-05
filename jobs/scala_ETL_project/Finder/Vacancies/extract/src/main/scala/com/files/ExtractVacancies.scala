package com.files

import com.files.URLHandler.{readOrDefault, readOrNone}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ExtractVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val vacsLimit: Int = getFromConfFile[Int]("vacsLimit")
    lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
    lazy val publicationTime: String = getFromConfFile[String]("publicationTime")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private def clusterUrl(l: Int, offset: Int): String = {
    val limit: Int = if (offset + l > conf.vacsLimit) conf.vacsLimit - offset else l
    s"https://api.finder.work/api/v2/vacancies/?categories=1&location=all&publication_time=${conf.publicationTime}&limit=$limit&offset=$offset"
  }
  private def vacancyUrl(id: Long): String = s"https://api.finder.work/api/v2/vacancies/$id"


  private val totalUrls: Int = math.min({
    val body: String = readOrDefault(clusterUrl(0, 0), conf.urlConf, """{"count": 0}""")
    """"count"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt
  }, conf.vacsLimit) / conf.vacsPerPage

  import spark.implicits._

  private val clusterUrlsDS: Dataset[String] = (0 to totalUrls)
    .map(value => clusterUrl(conf.vacsPerPage, conf.vacsPerPage * value)).toDS().repartition(conf.urlConf.requestsPS)

  private val clustersDS: Dataset[String] = clusterUrlsDS.mapPartitions(part => {
    part.flatMap(url => readOrNone(url, conf.urlConf))
  })


  private val vacancyIdsDF: DataFrame = spark.read.schema(StructType(Seq(StructField("items", ArrayType(StructType(Seq(StructField("id", LongType))))))))
    .json(clustersDS).select(explode(col("items")).as("item")).select(col("item.id").as("id")).repartition(conf.urlConf.requestsPS)

  private val vacanciesDS: Dataset[String] = vacancyIdsDF.mapPartitions(part => part.map(row => vacancyUrl(row.getLong(0)))).mapPartitions(part => {
    part.flatMap(url => readOrNone(url, conf.urlConf))
  }).repartition(conf.rawPartitions)


  vacanciesDS.write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

  spark.stop()

}
