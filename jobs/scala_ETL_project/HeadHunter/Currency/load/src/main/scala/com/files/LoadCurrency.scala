package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadCurrency extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.fileConf)

  give(
    conf = conf.fileConf,
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Dict(FolderName.Currency)
    ).get,
    tableName = conf.tableName(FolderName.Currency),
    saveMode = SaveMode.Append
  )

  stopSpark()
}