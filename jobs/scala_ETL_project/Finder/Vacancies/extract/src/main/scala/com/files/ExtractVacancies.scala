package com.files

import com.files.Common.URLConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ExtractVacancies extends SparkApp {

  private def vacancyURL(id: Long): String = s"https://api.finder.work/api/v1/vacancies/$id"

  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      lazy val vacsLimit: Int = getFromConfFile[Int]("vacsLimit")
      lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
      lazy val publicationTime: String = getFromConfFile[String]("publicationTime")
      lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

      verify()
    }

    val spark: SparkSession = defineSession(conf.sparkConf)

    // parameters
    val publicationTime: String = conf.publicationTime
    val urlConf: URLConf = conf.urlConf


    val clusterUrl: (Int, Int) => String = (l, offset) => {
      val limit: Int = if (offset + l > conf.vacsLimit) conf.vacsLimit - offset else l
      s"https://api.finder.work/api/v1/vacancies/?categories=1&location=all&publication_time=$publicationTime&limit=$limit&offset=$offset"
    }



    val totalUrls: Int = math.min({
      val body: String = URLHandler.readOrDefault(conf.urlConf, clusterUrl(0, 0), """{"count": 0}""")
      """"count"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt
    }, conf.vacsLimit) / conf.vacsPerPage

    import spark.implicits._

    val clusterUrlsDS: Dataset[String] = (0 to totalUrls)
      .map(value => clusterUrl(conf.vacsPerPage, conf.vacsPerPage * value)).toDS().repartition(urlConf.requestsPS)

    val clustersDS: Dataset[String] = clusterUrlsDS.mapPartitions(part => {
      part.flatMap(url => URLHandler.readOrNone(urlConf, url))
    })

    val vacancyIdsDF: DataFrame = spark.read.schema(StructType(Seq(StructField("items", ArrayType(StructType(Seq(StructField("id", LongType))))))))
      .json(clustersDS).select(explode(col("items")).as("item")).select(col("item.id").as("id")).repartition(urlConf.requestsPS)

    val vacanciesDS: Dataset[String] = vacancyIdsDF.mapPartitions(part => part.map(row => vacancyURL(row.getLong(0)))).mapPartitions(part => {
      part.flatMap(url => URLHandler.readOrNone(urlConf, url))
    }).repartition(conf.rawPartitions)

    vacanciesDS.write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

    spark.stop()

  }

}
