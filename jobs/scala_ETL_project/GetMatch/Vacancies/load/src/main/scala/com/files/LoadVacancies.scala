package com.files

import com.files.FolderName.FolderName
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends SparkApp {


  def main(args: Array[String]): Unit = {

    val conf: LocalConfig = new LocalConfig(args) { define() }
    val spark: SparkSession = defineSession(conf.commonConf)


    val loadWithConf = loadHelper(spark, conf)_

    loadWithConf(Seq("id", "name"), null, None, FolderName.Skills)
    loadWithConf(Seq("id", "city", "country"), null, None, FolderName.Locations)

    val vacs: DataFrame = HDFSHandler.load(spark, conf.commonConf)(FolderName.Vac)

    loadWithConf(
      Seq("id"),
      vacs.columns.toSeq.filterNot (col => Set("id", "publish_date").contains(col)),
      Some(vacs),
      FolderName.Vac
    )

    spark.stop()

  }


  private def loadHelper(spark: SparkSession, conf: LocalConfig)
                        (conflicts: Seq[String], updates: Seq[String], data: Option[DataFrame], folderName: FolderName): Unit = {

    val toSave: DataFrame = data match {
      case Some(df) => df
      case None => HDFSHandler.load(spark, conf.commonConf)(folderName)
    }

    DBHandler.save(conf.commonConf, toSave, conf.tableName(folderName), conflicts, updates)

  }

}