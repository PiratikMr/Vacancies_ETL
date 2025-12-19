package org.example.core.Interfaces.Services

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName

trait StorageService {

  def read(spark: SparkSession, folderName: FolderName): DataFrame

  def readText(spark: SparkSession, folderName: FolderName): Dataset[String]

  def write(df: DataFrame, folderName: FolderName): Unit

  def writeText(ds: Dataset[String], folderName: FolderName): Unit

}