package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadCurr extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.fileConf)

  give(
    conf = conf.fileConf,
    data = take(
      ss = ss,
      conf = conf.fileConf,
      fileName = "currency",
      FolderName.Dict
    ).get,
    tableName = "currency",
    saveMode = SaveMode.Append
  )

  stopSpark()
}