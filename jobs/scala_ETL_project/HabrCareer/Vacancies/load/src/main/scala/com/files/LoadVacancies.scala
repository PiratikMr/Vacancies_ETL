package com.files

import com.files.DBHandler.save
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: ProjectConfig = new ProjectConfig(args) { verify() }
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val vacanciesDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  private val updates: Seq[String] = vacanciesDF.columns.filterNot(col => Seq("id", "published_at").contains(col))
  DBHandler.save(conf.dbConf, vacanciesDF, FolderName.Vacancies, Some(Seq("id")), Some(updates))

  saveHelper(FolderName.Fields)
  saveHelper(FolderName.Skills)
  saveHelper(FolderName.Locations)

  spark.stop()


  private def saveHelper(
                          folderName: FolderName,
                          df: Option[DataFrame] = None
                        ): Unit = {
    val toSave: DataFrame = df match {
      case Some(value) => value
      case None => HDFSHandler.load(spark, conf.fsConf.getPath(folderName))
    }
    save(conf.dbConf, toSave, folderName, Some(Seq("id", "name")), None)
  }
}
