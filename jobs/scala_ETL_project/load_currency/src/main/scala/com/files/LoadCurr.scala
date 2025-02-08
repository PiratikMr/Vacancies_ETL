package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.LocalConfig
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadCurr extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.fileConf.spark)

  give(
    conf = conf.fileConf.db,
    data = take(
      ss = ss,
      conf = conf.fileConf.fs,
      fileName = "currency",
      isRoot = true,
    ).get,
    tableName = "currency",
    saveMode = SaveMode.Append
  )

  stopSpark()
}