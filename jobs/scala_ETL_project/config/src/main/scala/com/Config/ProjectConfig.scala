package com.Config

import com.typesafe.config.{Config, ConfigFactory}

class ProjectConfig(genConfFile: String, apiHeader: String, date: String) {
  private val conf: Config = ConfigFactory.load(genConfFile)

  lazy val api: APIConf = new APIConf(conf, "API.", apiHeader)
  lazy val db: DBConf = new DBConf(conf, "DB.")
  lazy val fs: FSConf = new FSConf(conf, "FS.", date)
  lazy val spark: SparkConf = new SparkConf(conf, "Spark.")
}
