package org.example.currency.implement

import org.apache.spark.sql.functions.{col, explode, from_json, to_json}
import org.apache.spark.sql.types.{DoubleType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.{FolderName, Stage}
import org.example.core.Interfaces.ETL.Transformer

object CurrencyTransformer extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.json(rawDS).as("data")

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {
      val transformed = rawDF.select(
        explode(
          from_json(
            to_json(col("data.conversion_rates")),
            MapType(StringType, DoubleType)
          )
        ).as(Seq("id", "rate"))
      )

      Map(FolderName(Stage, "Currency") -> transformed)
    }
}
