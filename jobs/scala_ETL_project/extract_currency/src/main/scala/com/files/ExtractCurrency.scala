package com.files

import EL.Load.give
import Spark.SparkApp
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object ExtractCurrency extends App with SparkApp {
  val df: DataFrame = toDF(takeURL("https://api.hh.ru/dictionaries").get)
  give(isRoot = true, df
    .withColumn("currency", explode(col("currency")))
    .select("currency.*")
    .withColumn("id", col("code"))
    .select("id", "name", "rate"), "currency")

  stopSpark()

  private def toDF(s: String): DataFrame = {
    import ss.implicits._
    ss.read.json(Seq(s).toDS())
  }
}
