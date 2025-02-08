package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.LocalConfig
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadVac extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf.spark)

  give(
    conf = conf.fileConf.db,
    tableName = "vacancies",
    data = take(
      ss = ss,
      conf = conf.fileConf.fs,
      fileName = conf.fileConf.fs.vacanciesTransformedFileName,
      isRoot = false
    ).get,
    saveMode = SaveMode.Append
  )

  stopSpark()
}