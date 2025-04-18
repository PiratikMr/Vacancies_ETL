package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadDictionaries extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  load(FolderName.Areas)
  load(FolderName.Currency)
  load(FolderName.Schedule)
  load(FolderName.Employment)
  load(FolderName.Experience)

  private val rolesDF: DataFrame = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Dict(FolderName.Roles)
  ).get
    .select("id", "name")
    .dropDuplicates("id")

  give(
    conf = conf.fileConf,
    data = rolesDF,
    tableName = conf.tableName(FolderName.Roles)
  )

  stopSpark()

  private def load(folderName: FolderName): Unit = give(
    conf = conf.fileConf,
    data = take(
      ss = ss,
      conf = conf.fileConf,
      folderName = FolderName.Dict(folderName)
    ).get,
    tableName = conf.tableName(folderName)
  )
}