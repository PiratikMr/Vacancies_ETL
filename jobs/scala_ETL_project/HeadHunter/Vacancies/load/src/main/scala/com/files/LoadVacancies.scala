package com.files

import com.files.DBHandler.save
import com.files.FolderName.FolderName
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadVacancies extends SparkApp {

  def main(args: Array[String]): Unit = {

    val conf: LocalConfig = new LocalConfig(args) { define() }
    val spark: SparkSession = defineSession(conf.commonConf)

    loadData(spark, conf)

    spark.stop()

  }


  private def loadData(spark: SparkSession, conf: LocalConfig): Unit = {

    val loadWithConf = loadHelper(spark, conf)_
    val loadWithConflict = loadWithConf(Seq("id"), None, Seq("name"))

    loadWithConflict(FolderName.Employer)
    loadWithConflict(FolderName.Skills)

    val vacancies: DataFrame = HDFSHandler.load(spark = spark, conf = conf.commonConf)(FolderName.Vac)

    loadWithConf(
      Seq("id"),
      Some(vacancies),
      vacancies.columns.toSeq.filterNot(col => Set("id", "publish_date").contains(col))
    )(FolderName.Vac)

  }


  private def loadHelper(spark: SparkSession, conf: LocalConfig)
                        (conflicts: Seq[String], data: Option[DataFrame], updates: Seq[String])
                        (folderName: FolderName): Unit = {

    val toSave: DataFrame = data match {
      case Some(value) => value
      case None => HDFSHandler.load(spark = spark, conf = conf.commonConf)(folderName)
    }

    save(
      conf = conf.commonConf,
      data = toSave,
      tableName = conf.tableName(folderName),
      conflicts = conflicts,
      updates = updates
    )
  }

}