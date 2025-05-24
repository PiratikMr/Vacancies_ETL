package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    lazy val vacsLimit: Int = getFromConfFile[Int]("vacsLimit")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }
  override val ss: SparkSession = defineSession(conf.commonConf)

  import ss.implicits._

  private val firstTake: DataFrame = ss.read
    .json(Seq(takeURL(url(1, 1), conf.commonConf).get).toDS())
  private val total: Long = firstTake
    .select(col("meta.total"))
    .first()
    .getLong(0)

  private val vacsToProcess: Long = Math.min(total, conf.vacsLimit)

  println(s"Vacancies to process: $vacsToProcess")

  private val data: String = takeURL(url(0, vacsToProcess + 2), conf.commonConf).get

  private val df: DataFrame = ss.read
    .json(Seq(data).toDS())
    .withColumn("offers", explode(col("offers")))
    .select("offers.*")

  println(s"Read vacancies: ${df.count()}")

  give(
    conf = conf.commonConf,
    folderName = FolderName.Raw,
    data = df.repartition(conf.rawPartitions)
  )

  stopSpark()

  private def url(offset: Long, limit: Long): String = {
    s"https://getmatch.ru/api/offers?offset=$offset&limit=$limit"
  }
}
