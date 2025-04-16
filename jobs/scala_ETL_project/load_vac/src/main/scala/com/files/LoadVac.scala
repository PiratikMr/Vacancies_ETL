package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadVac extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  give(
    conf = conf.fileConf,
    tableName = "employers",
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Employer
    ).get,
    saveMode = SaveMode.Append
  )

  give(
    conf = conf.fileConf,
    tableName = "skills",
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Skills
    ).get,
    saveMode = SaveMode.Append
  )

  give(
    conf = conf.fileConf,
    tableName = "vacancies",
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Trans
    ).get,
    saveMode = SaveMode.Append
  )

  stopSpark()
}