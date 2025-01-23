package com.Config

object SparkConfig extends LocalConfig {
  val name: String = getStringField("Spark.name")
  val master: String = getStringField("Spark.master")
}
