package com.files

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.util.{Failure, Success, Try}

object HDFSHandler {

  def load(spark: SparkSession, filePath: String)
  : DataFrame = {

    Try(
      spark.read.parquet(filePath)
    ) match {
      case Failure(exception) => throw exception
      case Success(value) => value
    }

  }

  def saveParquet(df: DataFrame, filePath: String)
  : Unit = {

    Try (
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(filePath)
    ) match {
      case Failure(exception) => throw exception
      case Success(_) => ()
    }

  }

}
