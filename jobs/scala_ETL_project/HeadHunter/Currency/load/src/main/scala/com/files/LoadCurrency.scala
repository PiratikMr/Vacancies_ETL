package com.files

import EL.Extract.take
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB.{give, load, save}
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadCurrency extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.commonConf)

  save(
    conf = conf.commonConf,
    data = take(
      ss = ss,
      conf = conf.commonConf,
      folderName = FolderName.Dict(FolderName.Currency)
    ).get,
    tableName = FolderName.Currency,
    conflicts = Seq("id"),
    updates = Seq("rate")
  )

  stopSpark()
}