package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.FolderName.{FolderName, isDict}
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.{give, load, save}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args, "gm") {
    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  loadData(FolderName.Skills, Seq("id", "name"))
  loadData(FolderName.Locations, Seq("id", "city", "country"))

  private val vac: DataFrame = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Vac
  ).get
  private val notUpd: Set[String] = Set("id", "publish_date")
  loadData(FolderName.Vac, Seq("id"), data = vac, updates = vac.columns.toSeq.filter(col => !notUpd.contains(col)))



  stopSpark()

  private def loadData(folderName: FolderName, conflicts: Seq[String], updates: Seq[String] = null, data: DataFrame = null): Unit = {
    save(
      conf = conf.fileConf,
      data = if (data == null) take(
        ss = ss,
        conf = conf.fileConf,
        folderName = folderName
      ).get else data,
      tableName = conf.tableName(folderName),
      conflicts = conflicts,
      updates = updates
    )
  }
}