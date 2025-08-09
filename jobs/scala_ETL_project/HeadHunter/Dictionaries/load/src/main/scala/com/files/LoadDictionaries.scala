package com.files

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

object LoadDictionaries extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {

    private val allDictionaries: String = "areas roles dict:curr dict:schd dict:empl dict:expr"
    val dictionaries: ScallopOption[String] = opt[String](name = "dictionaries", default = Some(allDictionaries))

    define()
  }
  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val dictSet: Set[String] = conf.dictionaries().split(" ").map(_.trim.toLowerCase).toSet


  dictSet.foreach {
    case "areas" => saveHelper(FolderName.Areas)
    case "roles" => {
      val df: DataFrame = HDFSHandler.load(spark, conf.fsConf.getPath(FolderName.Roles)).select("id", "name")
        .dropDuplicates("id")
      saveHelper(FolderName.Roles, Some(Seq("name")), Some(df))
    }
    case s if s.startsWith("dict:") =>
      s.stripPrefix("dict:") match {
        case "curr" => saveHelper(FolderName.Currency, Some(Seq("rate")))
        case "schd" => saveHelper(FolderName.Schedule)
        case "empl" => saveHelper(FolderName.Employment)
        case "expr" => saveHelper(FolderName.Experience)
      }
  }

  spark.stop()


  private def saveHelper(folderName: FolderName, updates: Option[Seq[String]] = Some(Seq("name")), df: Option[DataFrame] = None): Unit = {
    val toSave: DataFrame = df match {
      case Some(value) => value
      case None => HDFSHandler.load(spark, conf.fsConf.getPath(folderName))
    }
    DBHandler.save(toSave, conf.dbConf, folderName, Seq("id"), updates)
  }
}