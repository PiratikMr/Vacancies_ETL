package com.files

import com.files.FolderName.FolderName
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

object LoadDictionaries extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {

    private val allDictionaries: String = "areas roles dict:curr dict:schd dict:empl dict:expr"
    val dictionaries: ScallopOption[String] = opt[String](name = "dictionaries", default = Some(allDictionaries))

    define()
  }


  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)

    val dictSet: Set[String] = conf.dictionaries().split(" ").map(_.trim.toLowerCase).toSet

    val loadWithConf = loadHelper(spark, conf)_
    val loadDef = loadWithConf(Seq("name"), None)

    dictSet.foreach(arg => {
      arg.trim.toLowerCase match {
        case "areas" => loadDef(FolderName.Areas)
        case "roles" => processRoles(spark, conf)
        case s if s.startsWith("dict:") =>
          s.stripPrefix("dict:") match {
            case "curr" => loadWithConf(Seq("rate"), None)(FolderName.Currency)
            case "schd" => loadDef(FolderName.Schedule)
            case "empl" => loadDef(FolderName.Employment)
            case "expr" => loadDef(FolderName.Experience)
          }
      }
    })

    spark.stop()

  }

  private def processRoles(spark: SparkSession, conf: LocalConfig): Unit = {
    val rolesDF: DataFrame = HDFSHandler.load(spark = spark, conf = conf.commonConf)(FolderName.Roles)
      .select("id", "name")
      .dropDuplicates("id")

    loadHelper(spark, conf)(Seq("name"), Some(rolesDF))(FolderName.Roles)
  }


  private def loadHelper(spark: SparkSession, conf: LocalConfig)
                        (updates: Seq[String], data: Option[DataFrame])
                        (folderName: FolderName): Unit = {
    val toSave: DataFrame = data match {
      case Some(df) => df
      case None => HDFSHandler.load(spark = spark, conf = conf.commonConf)(folderName = folderName)
    }

    DBHandler.save(
      conf = conf.commonConf,
      data = toSave,
      tableName = conf.tableName(folderName),
      conflicts = Seq("id"),
      updates = updates
    )
  }

}