package com.files

import com.files.FolderName.FolderName
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object HDFSHandler {

  def load(spark: SparkSession, conf: CommonConfig)
          (folderName: FolderName)
  : DataFrame = {

    Try(
      spark.read.parquet(conf.fs.getPath(folderName))
    ) match {
      case Failure(exception) => throw exception
      case Success(value) => value
    }

  }


  def save(conf: CommonConfig)
          (folderName: FolderName, data: DataFrame)
  : Unit = {

    Try (
      data.write
        .mode(SaveMode.Overwrite)
        .parquet(conf.fs.getPath(folderName))
    ) match {
      case Failure(exception) => throw exception
      case Success(_) => ()
    }

  }

}
