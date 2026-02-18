package org.example.core.etl

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Transformer {

  def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame

  def transform(spark: SparkSession, rawDF: DataFrame): DataFrame

  def normalize(spark: SparkSession, transformedData: DataFrame): DataFrame

}
