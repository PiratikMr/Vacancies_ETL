package com.files

import com.files.FolderName.FolderName
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends SparkApp {

  def main(args: Array[String]): Unit = {

    val conf: LocalConfig = new LocalConfig(args) { define() }
    val spark: SparkSession = defineSession(conf.commonConf)

    saveData(spark, conf)

    spark.stop()

  }


  private def saveData(spark: SparkSession, conf: LocalConfig): Unit = {

    val saveWithConf = saveHelper(spark, conf)_

    val saveDef = saveWithConf(Seq("id", "name"), None)

    saveDef(FolderName.Locations)
    saveDef(FolderName.JobFormats)
    saveDef(FolderName.Skills)
    saveDef(FolderName.Fields)
    saveDef(FolderName.Levels)

    saveWithConf(Seq("id"), Some(Seq("id")))(FolderName.Vac)
  }


  private def saveHelper(spark: SparkSession, conf: LocalConfig)
                        (conflicts: Seq[String], notUpdates: Option[Seq[String]])
                        (folderName: FolderName): Unit = {

    val toSave: DataFrame = HDFSHandler.load(spark, conf.commonConf)(folderName)

    val updates: Seq[String] = notUpdates match {
      case Some(seq) => toSave.columns.toSeq.filterNot(col => seq.contains(col))
      case None => null
    }

    DBHandler.save(
      conf = conf.commonConf,
      data = toSave,
      tableName = conf.tableName(folderName),
      conflicts = conflicts,
      updates = updates
    )
  }

}