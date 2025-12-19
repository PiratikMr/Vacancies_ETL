package org.example.core

import org.apache.spark.sql.SparkSession
import org.example.config.Cases.Structures.SparkConf

object SparkApp {

  def defineSession(conf: SparkConf, etlPart: String): SparkSession =
    SparkSession.builder()
      .appName(s"${conf.name}_$etlPart")
      .master(conf.master)
      .config("spark.driver.memory", conf.driverMemory)
      .config("spark.driver.cores", conf.driverCores)
      .config("spark.executor.memory", conf.executorMemory)
      .config("spark.executor.cores", conf.executorCores)
      .getOrCreate()
}
