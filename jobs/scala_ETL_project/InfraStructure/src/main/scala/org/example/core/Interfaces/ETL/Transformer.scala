package org.example.core.Interfaces.ETL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName

trait Transformer {

  def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame

  def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame]

}
