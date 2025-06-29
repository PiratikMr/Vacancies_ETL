package com.files

import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  def defineSession(conf: CommonConfig): SparkSession = {
    SparkSession.builder()
      .appName(conf.spark.name)
      .master(conf.spark.master)
      .getOrCreate()
  }

}
