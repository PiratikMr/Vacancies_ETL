package com.files

import com.files.DBHandler.save
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: ProjectConfig = new ProjectConfig(args) { verify() }
  private val spark: SparkSession = defineSession(conf.sparkConf)


  private val vacDf: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  private val updates: Seq[String] = vacDf.columns.filterNot(col => Set("id", "publish_date").contains(col))

  private val baseConflicts: Seq[String] = Seq("id")

  saveHelper(FolderName.Employers, Seq("id"), Some(Seq("name", "trusted")))

  saveHelper(FolderName.Vacancies, baseConflicts, Some(updates), Some(vacDf))

  saveHelper(FolderName.Skills, baseConflicts :+ "name", None)
  //saveHelper(FolderName.DriverLicenses, baseConflicts, None)
  saveHelper(FolderName.Languages, baseConflicts ++ Seq("name", "level"), None)


  spark.stop()


  private def saveHelper(
                          folderName: FolderName,
                          conflicts: Seq[String],
                          updates: Option[Seq[String]],
                          df: Option[DataFrame] = None
                        ): Unit = {
    val toSave: DataFrame = df match {
      case Some(value) => value
      case None => HDFSHandler.load(spark, conf.fsConf.getPath(folderName))
    }
    save(conf.dbConf, toSave, folderName, Some(conflicts), updates)
  }

}