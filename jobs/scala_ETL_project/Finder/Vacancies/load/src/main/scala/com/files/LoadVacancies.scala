package com.files

import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends App with SparkApp {

  private val conf: LocalConfig = new LocalConfig(args) { define() }
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val vacanciesDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Vacancies))
  private val updates: Seq[String] = vacanciesDF.columns.filterNot(col => Seq("id").contains(col))
  DBHandler.save(conf.dbConf, vacanciesDF, FolderName.Vacancies, Some(Seq("id")), Some(updates))

  private val locationsDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Locations))
  DBHandler.save(conf.dbConf, locationsDF, FolderName.Locations, Some(Seq("id", "name")), None)

  private val fieldsDF: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Fields))
  DBHandler.save(conf.dbConf, fieldsDF, FolderName.Fields, Some(Seq("id", "name")), None)

  spark.stop()

}
