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

  loadData(FolderName.Employer)
  loadData(FolderName.Skills)
  loadData(FolderName.Vac)

  stopSpark()

  private def loadData(folderName: FolderName): Unit = {
    give(
      conf = conf.fileConf,
      tableName = conf.tableName(folderName),
      data = getData(folderName),
      saveMode = SaveMode.Append
    )
  }

  private def getData(folderName: FolderName): DataFrame = {
    take(
      ss = ss,
      conf = conf.fileConf,
      folderName = folderName
    ).get
  }
}