package org.example.TestData

import org.apache.spark.sql.SparkSession

object TestObjects {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LocalSpark")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()

}
