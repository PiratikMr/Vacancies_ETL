package com.Config

object SparkConfig extends LocalConfig {
  lazy val name: String = getString("spark.name")
  lazy val master: String = getString("spark.master")
}
