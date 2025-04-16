package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadDict extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  load("areas")
  load("currency")
  load("schedule")
  load("employment")
  load("experience")

  private val rolesDF: DataFrame = take(
    ss = ss,
    conf = conf.fileConf,
    fileName = "roles",
    folderName = FolderName.Dict
  ).get
    .select("id", "name")
    .dropDuplicates("id")

  give(
    conf = conf.fileConf,
    data = rolesDF,
    tableName = "roles"
  )

  stopSpark()

  private def load(name: String): Unit = give(
    conf = conf.fileConf,
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Dict,
      fileName = name
    ).get,
    tableName = name
  )
}