package com.files

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: ProjectConfig = new ProjectConfig(args) { verify() }
  private val spark: SparkSession = defineSession(conf.sparkConf)


  private val vacanciesDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  private val updates: Seq[String] = vacanciesDF.columns.filterNot(col => Seq("id", "published_at").contains(col))

  DBHandler.save(conf.dbConf, vacanciesDF, FolderName.Vacancies, Some(Seq("id")), Some(updates))

  private val skillsDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Skills))

  DBHandler.save(conf.dbConf, skillsDF, FolderName.Skills, Some(Seq("id", "name")), None)

  spark.stop()
}