package com.files

import URLHandler._
import com.files.Common.URLConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ExtractVacancies extends SparkApp {

  private def vacancyUrl(id: Long): String = s"https://getmatch.ru/api/offers/$id"


  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      lazy val vacsLimit: Int = math.max(1, getFromConfFile[Int]("vacsLimit"))
      lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
      lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")
      lazy val inDays: String = getFromConfFile[String]("inDays")

      verify()
    }

    val spark: SparkSession = defineSession(conf.sparkConf)
    import spark.implicits._

    // parameters
    val urlConf: URLConf = conf.urlConf


    val clusterUrl: (Int, Int) => String = (l, offset) => {
      val limit: Int = if (offset + l > conf.vacsLimit) conf.vacsLimit - offset else l
      s"https://getmatch.ru/api/offers?pa=${conf.inDays}d&limit=$limit&offset=$offset"
    }


    val totalUrls: Int = math.min({
      val body: String = URLHandler.readOrDefault(urlConf, clusterUrl(1, 0), """{"total":0}""")
      """"total"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt
    }, conf.vacsLimit) / conf.vacsPerPage


    val clusterUrlsDS: Dataset[String] = (0 to totalUrls)
      .map(value => clusterUrl(conf.vacsPerPage, conf.vacsPerPage * value)).toDS().repartition(conf.urlConf.requestsPS)

    val clustersDS: Dataset[String] = clusterUrlsDS.mapPartitions(part => {
      URLHandler.useClient[String, String](part, (backend, url) => {
        URLHandler.readOrNone(urlConf, url, Some(backend))
      })
    })


    val vacancyIdsDF: DataFrame = spark.read.schema(StructType(Seq(StructField("offers", ArrayType(StructType(Seq(StructField("id", LongType))))))))
      .json(clustersDS).select(explode(col("offers")).as("offer")).select(col("offer.id").as("id")).repartition(conf.urlConf.requestsPS)


    val vacanciesDS: Dataset[String] = vacancyIdsDF.mapPartitions(part => part.map(row => vacancyUrl(row.getLong(0)))).mapPartitions(part => {
      URLHandler.useClient[String, String](part, (backend, url) => {
        URLHandler.readOrNone(urlConf, url, Some(backend))
      })
    }).repartition(conf.rawPartitions)


    vacanciesDS.write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

    spark.stop()
  }

}
