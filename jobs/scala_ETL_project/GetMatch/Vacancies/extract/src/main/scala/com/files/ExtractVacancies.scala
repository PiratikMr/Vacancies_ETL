package com.files

import com.files.URLHandler.{readURL, requestError}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractVacancies extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val vacsLimit: Int = getFromConfFile[Int]("vacsLimit")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }

  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)

    val dataDF: DataFrame = getData(spark, conf)

    println(s"Total vacancies: ${dataDF.count}")

    HDFSHandler.save(conf.commonConf)(FolderName.Raw, dataDF.repartition(conf.rawPartitions))

    spark.stop()

  }


  private def getData(spark: SparkSession, conf: Conf): DataFrame = {

    import spark.implicits._

    val firstTake: DataFrame = spark.read.json(Seq(
      readURL(url(1, 1), conf.commonConf) match {
        case Some(body) => body
        case _ => requestError(url(1, 1))
      }
    ).toDS)
    val total: Long = math.min(
      conf.vacsLimit,
      firstTake.select(col("meta.total")).first.getLong(0)
    ) + 2

    println(s"Vacancies to read: $total")

    val bodyData: String = readURL(url(0, total), conf.commonConf) match {
      case Some(body) => body
      case _ => requestError(url(0, total))
    }

    spark.read
      .json(Seq(bodyData).toDS())
      .withColumn("offers", explode(col("offers")))
      .select("offers.*")

  }


  private def url(offset: Long, limit: Long): String = {
    s"https://getmatch.ru/api/offers?offset=$offset&limit=$limit"
  }
}
