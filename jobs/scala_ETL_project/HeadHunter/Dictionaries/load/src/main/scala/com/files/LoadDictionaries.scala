package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.{give, save}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadDictionaries extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }

  override val ss: SparkSession = defineSession(conf.commonConf)

  loadData(FolderName.Areas, Seq("id"), updates = Seq("name"))
  loadData(FolderName.Currency, Seq("id"), updates = Seq("rate"), isDict = true)
  loadData(FolderName.Schedule, Seq("id"), updates = Seq("name"))
  loadData(FolderName.Employment, Seq("id"), updates = Seq("name"))
  loadData(FolderName.Experience, Seq("id"), updates = Seq("name"))

  private val rolesDF: DataFrame = take(
    ss = ss,
    conf = conf.commonConf,
    folderName = FolderName.Dict(FolderName.Roles)
  ).get
    .select("id", "name")
    .dropDuplicates("id")

  loadData(FolderName.Roles, Seq("id"), updates = Seq("name"), data = rolesDF)

  stopSpark()

  private def loadData(folderName: FolderName, conflicts: Seq[String], updates: Seq[String] = null, isDict: Boolean = false, data: DataFrame = null): Unit = {
    save(
      conf = conf.commonConf,
      data = if (data == null) take(
        ss = ss,
        conf = conf.commonConf,
        folderName = FolderName.Dict(folderName)
      ).get else data,
      tableName = if (isDict) folderName else conf.tableName(folderName),
      conflicts = conflicts,
      updates = updates
    )
  }
}