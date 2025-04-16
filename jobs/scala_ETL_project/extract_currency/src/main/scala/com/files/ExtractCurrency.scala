package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractCurrency extends App with SparkApp {

  private val conf = new LocalConfig(args, "HHapi") {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  private val currencyDF: DataFrame = toDF(takeURL("https://api.hh.ru/dictionaries", conf.fileConf).get)
  give(
    conf = conf.fileConf,
    fileName = "currency",
    folderName = FolderName.Dict,
    data = currencyDF.withColumn("currency", explode(col("currency")))
      .select("currency.*")
      .withColumn("id", col("code"))
      .select("id", "name", "rate")
  )

  stopSpark()

  private def toDF(s: String): DataFrame = {
    import ss.implicits._
    ss.read.json(Seq(s).toDS())
  }
}