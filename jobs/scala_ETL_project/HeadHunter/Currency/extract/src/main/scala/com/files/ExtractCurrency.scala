package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.{requestError, takeURL}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractCurrency extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.commonConf)


  private val currencyDF: DataFrame = toDF(
    takeURL("https://api.hh.ru/dictionaries", conf.commonConf) match {
      case Some(body) => body
      case _ => requestError("https://api.hh.ru/dictionaries")
    }
  )
  give(
    conf = conf.commonConf,
    folderName = FolderName.Dict(FolderName.Currency),
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