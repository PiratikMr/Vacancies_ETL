package com.files

import EL.Extract.take
import com.LoadDB.LoadDB.give
import Spark.SparkApp
import com.Config.HDFSConfig
import org.apache.spark.sql.SaveMode
import org.rogach.scallop.{ScallopConf, ScallopOption}

object LoadVac extends App with SparkApp {

  private val conf = new ScallopConf(args) {
    val vacanciesInFileName: ScallopOption[String] = opt[String](default = Some(HDFSConfig.vacanciesTransformedFileName))
    val vacanciesTableName: ScallopOption[String] = opt[String](default = Some("vacancies"))

    verify()
  }


  give(take(isRoot = false, fileName = conf.vacanciesInFileName()).get, tableName = conf.vacanciesTableName(), saveMode = SaveMode.Append)

  stopSpark()
}
