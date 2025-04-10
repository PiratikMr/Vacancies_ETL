package com.LoadDB

import com.Config.ProjectConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.Serializable
import scala.util.Try

object LoadDB extends Serializable {
  def give(
            conf: ProjectConfig,
            data: DataFrame,
            tableName: String,
            saveMode: SaveMode = SaveMode.Overwrite
          ): Try[Unit] = {
    Try(
      data.write
        .format("jdbc")
        .option("truncate", value = true)
        .option("driver", "org.postgresql.Driver")
        .option("url", conf.db.DBurl)
        .option("user", conf.db.userName)
        .option("password", conf.db.userPassword)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
    )
  }
}
