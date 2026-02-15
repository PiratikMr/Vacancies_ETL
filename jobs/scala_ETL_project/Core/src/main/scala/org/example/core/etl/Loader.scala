package org.example.core.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Loader {

  def load(spark: SparkSession, df: DataFrame): Unit

}