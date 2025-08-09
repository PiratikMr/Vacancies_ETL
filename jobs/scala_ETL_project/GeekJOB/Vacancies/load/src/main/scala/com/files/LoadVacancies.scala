package com.files

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: LocalConfig = new LocalConfig(args) { define() }
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val vacsDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  DBHandler.save(vacsDF, conf.dbConf, FolderName.Vacancies, Seq("id"), Some(vacsDF.columns.filterNot(_.equals("id"))))

  saveHelper(FolderName.Fields)
  saveHelper(FolderName.JobFormats)
  saveHelper(FolderName.Levels)
  saveHelper(FolderName.Locations)
  saveHelper(FolderName.Skills)

  spark.stop()


  private def saveHelper(fn: FolderName, cnf: Seq[String] = Seq("id", "name"), upd: Option[Seq[String]] = None): Unit = {
    val toSave: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(fn))
    DBHandler.save(toSave, conf.dbConf, fn, cnf, upd)
  }
}