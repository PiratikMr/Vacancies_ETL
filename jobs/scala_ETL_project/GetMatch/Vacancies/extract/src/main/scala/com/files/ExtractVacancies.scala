package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

object ExtractVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args, "gm") {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(6), validate = _ > 0)

    define()
  }
  override val ss: SparkSession = defineSession(conf.fileConf)

  import ss.implicits._

  private val firstTake: DataFrame = ss.read
    .json(Seq(takeURL(url(1, 1), conf.fileConf).get).toDS())
  private val total: Long = firstTake
    .select(col("meta.total"))
    .first()
    .getLong(0)

  println(s"Total vacancies: $total")

  private val date: String = takeURL(url(0, total + 2), conf.fileConf).get

  private val df: DataFrame = ss.read
    .json(Seq(date).toDS())
    .withColumn("offers", explode(col("offers")))
    .select("offers.*")

  println(s"Read vacancies: ${df.count()}")

  give(
    conf = conf.fileConf,
    folderName = FolderName.Raw,
    data = df
  )

  stopSpark()

  private def url(offset: Long, limit: Long): String = {
    s"https://getmatch.ru/api/offers?offset=$offset&limit=$limit"
  }
}
