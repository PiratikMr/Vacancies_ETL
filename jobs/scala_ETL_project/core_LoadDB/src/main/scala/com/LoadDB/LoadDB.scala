package com.LoadDB

import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

object LoadDB {
  def give(data: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Try[Unit] = {
    import com.Config.DBConfig
    Try(
      data.write
        .format("jdbc")
        .option("truncate", value = true)
        .option("driver", "org.postgresql.Driver")
        .option("url", DBConfig.DBurl)
        .option("dbtable", tableName)
        .option("user", DBConfig.userName)
        .option("password", DBConfig.userPassword)
        .mode(saveMode)
        .save()
    )
  }
}
