package com.files

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: LocalConfig = new LocalConfig(args) { define() }
  private val spark: SparkSession = defineSession(conf.sparkConf)


  private val vacanciesDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  private val updates: Seq[String] = vacanciesDF.columns.filterNot(col => Seq("id", "published_at").contains(col))

  DBHandler.save(vacanciesDF, conf.dbConf, FolderName.Vacancies, Seq("id"), Some(updates))

  private val skillsDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Skills))

  DBHandler.save(skillsDF, conf.dbConf, FolderName.Skills, Seq("id", "name"), None)

  spark.stop()
}