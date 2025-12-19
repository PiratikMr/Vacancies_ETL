package org.example.headhunter.dictionaries.implement

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer
import org.example.headhunter.dictionaries.config.HHFolderNames

object RolesTransformer extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {
      val rolesDF = rawDF
        .withColumn("categories", explode(col("categories"))).select("categories.*")
        .withColumn("id", col("id").cast(LongType))
        .withColumn("roles", explode(col("roles")))
        .select(col("id").as("field_id"),
          col("roles.id").cast(LongType).as("id"),
          col("roles.name").as("name"))
        .dropDuplicates("id")

      Map(HHFolderNames.roles.stage -> rolesDF)
    }
}
