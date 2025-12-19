package org.example.headhunter.dictionaries.implement

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer
import org.example.headhunter.dictionaries.config.HHFolderNames

object DictionariesTransformer extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] = {
    Map(
      HHFolderNames.schedule.stage -> getDF("schedule", rawDF),
      HHFolderNames.employment.stage -> getDF("employment", rawDF),
      HHFolderNames.experience.stage -> getDF("experience", rawDF)
    )
  }
  private def getDF(field: String, rawDF: DataFrame): DataFrame = {
    rawDF
      .withColumn(s"$field", explode(col(s"$field")))
      .select(s"$field.*")
      .select("id", "name")
      .dropDuplicates("id", "name")
      .repartition(1)
  }
}