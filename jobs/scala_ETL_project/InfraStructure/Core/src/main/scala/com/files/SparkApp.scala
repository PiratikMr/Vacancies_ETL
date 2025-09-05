package com.files

import com.files.Common.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  def defineSession(conf: SparkConf): SparkSession = {
    SparkSession.builder()
      .appName(conf.name)
      .master(conf.master)
      .config("spark.driver.memory", conf.driverMemory)
      .config("spark.driver.cores", conf.driverCores)
      .config("spark.executor.memory", conf.executorMemory)
      .config("spark.executor.cores", conf.executorCores)
      .getOrCreate()
  }

}
