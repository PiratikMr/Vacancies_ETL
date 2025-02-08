package com.LoadDB

import com.Config.DBConf
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.Serializable
import scala.util.Try

object LoadDB extends Serializable {
  def give(
            conf: DBConf,
            data: DataFrame,
            tableName: String,
            saveMode: SaveMode = SaveMode.Overwrite
          ): Try[Unit] = {
    Try(
      data.write
        .format("jdbc")
        .option("truncate", value = true)
        .option("driver", "org.postgresql.Driver")
        .option("url", conf.DBurl)
        .option("user", conf.userName)
        .option("password", conf.userPassword)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
    )
  }
}
