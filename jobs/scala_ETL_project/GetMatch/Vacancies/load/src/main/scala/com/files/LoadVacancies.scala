package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  loadData(FolderName.Skills)
  loadData(FolderName.Locations)
  loadData(FolderName.Vac)

  stopSpark()

  private def loadData(folderName: FolderName): Unit = {
    give(
      conf = conf.fileConf,
      tableName = conf.tableName(folderName),
      data = take(
        ss = ss,
        conf = conf.fileConf,
        folderName = folderName
      ).get,
      saveMode = SaveMode.Append
    )
  }
}