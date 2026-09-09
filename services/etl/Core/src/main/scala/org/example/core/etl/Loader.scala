package org.example.core.etl

import org.apache.spark.sql.SparkSession
import org.example.core.etl.model.NormalizationResult

trait Loader {

  def load(spark: SparkSession, result: NormalizationResult): Unit

}
