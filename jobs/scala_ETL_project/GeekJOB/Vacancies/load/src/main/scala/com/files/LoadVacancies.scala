package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.FolderName.{FolderName, isDict}
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.{give, load, save}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args, "gj") {
    define()
  }

  override val ss: SparkSession = defineSession(conf.commonConf)

  loadData(folderName = FolderName.Vac, conflicts = Seq("id"), notUpdates = Seq("id"))
  loadData(folderName = FolderName.Locations)
  loadData(folderName = FolderName.JobFormats)
  loadData(folderName = FolderName.Skills)
  loadData(folderName = FolderName.Fields)
  loadData(folderName = FolderName.Levels)

  stopSpark()

  private def loadData(folderName: FolderName, conflicts: Seq[String] = Seq("id", "name"), notUpdates: Seq[String] = null): Unit = {

    val d = take(
      ss = ss,
      conf = conf.commonConf,
      folderName = folderName
    ).get

    val updates: Seq[String] = if (notUpdates == null) null
    else {
      d.columns.toSeq.filter(col => !notUpdates.contains(col))
    }

    save(
      conf = conf.commonConf,
      data = d,
      tableName = conf.tableName(folderName),
      conflicts = conflicts,
      updates = updates
    )
  }
}