package org.example.core.Interfaces.Services

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.config.FolderName.FolderName

trait DataBaseService {

  def load(spark: SparkSession, targetTable: String): DataFrame

  def getActiveVacancies(spark: SparkSession, limit: Int): DataFrame

  def updateActiveVacancies(ids: DataFrame): Unit

  def save(df: DataFrame,
           folderName: FolderName,
           conflicts: Seq[String],
           updates: Option[Seq[String]] = None): Unit
}